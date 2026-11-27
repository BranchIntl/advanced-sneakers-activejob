# frozen_string_literal: true

require 'logger'
require 'timeout'
require 'active_job'
require 'active_job/queue_adapters'

# Repros and coverage for the v0.8.0 delayed-delivery changes (#11, #12, #13),
# run against a real broker. Examples marked `pending` prove a defect: they
# fail today and RSpec reports them as pending; once the defect is fixed they
# pass and RSpec flags them so the `pending` can be removed.
describe 'Delayed delivery v0.8.0', :rabbitmq do
  let(:logger) { Logger.new(IO::NULL) }
  let(:connection) { Bunny.new(ENV.fetch('RABBITMQ_URL')).start }
  let(:http) { RabbitmqHelpers.http_api }
  let(:delivery_exchange) { AdvancedSneakersActiveJob::LeveledDelayedPublisher::DELIVERY_EXCHANGE }

  around do |example|
    previous_logger = ::ActiveJob::Base.logger
    ::ActiveJob::Base.logger = logger
    example.run
  ensure
    ::ActiveJob::Base.logger = previous_logger
  end

  after { connection.close if connection.open? }

  def leveled_publisher(levels: 20)
    AdvancedSneakersActiveJob::LeveledDelayedPublisher.new(exchange: 'activejob', levels: levels, connection: connection).tap do |publisher|
      allow(publisher).to receive(:logger).and_return(logger)
    end
  end

  def with_channel
    channel = connection.create_channel
    yield channel
  ensure
    channel.close if channel&.open?
  end

  def message_count(queue)
    with_channel { |channel| channel.queue_declare(queue, passive: true).message_count }
  end

  def wait_until(timeout:)
    deadline = Time.now + timeout
    loop do
      result = yield
      return result if result || Time.now > deadline

      sleep 0.05
    end
  end

  describe 'Publisher mandatory republish when the delay.delivery.x bind fails at channel level (#13)' do
    let(:publisher) do
      AdvancedSneakersActiveJob::Publisher.new(
        exchange: 'activejob',
        exchange_options: { type: 'direct', durable: true },
        connection: connection
      )
    end

    before do
      allow(publisher).to receive(:logger).and_return(logger)
      allow(AdvancedSneakersActiveJob.config).to receive(:handle_unrouted_messages).and_return(true)
      # An inequivalent delay.delivery.x makes the override's topic redeclare
      # fail with 406, which closes the channel that sent it. A 403 from a
      # missing configure permission closes the channel the same way.
      with_channel { |channel| channel.fanout(delivery_exchange, durable: true) }
    end

    it 'keeps the publisher usable for other publishes' do
      pending 'defect: the bind runs on the shared publish channel, the rescue hides the close, ' \
              'and with_errors_handling retries setup_queue_for_republish forever while holding @mutex'

      resets = 0
      allow(publisher).to receive(:reset_exchange!).and_wrap_original do |original|
        resets += 1
        original.call
      end

      publisher.publish('unrouted', routing_key: 'ghost_orders')
      sleep 0.5 # let the returned-message thread take @mutex

      second = Thread.new { publisher.publish('second', routing_key: 'ghost_orders') }
      finished = second.join(3)

      begin
        expect(finished).not_to be_nil,
                                "second publish still blocked after 3s; reset_exchange! ran #{resets} times"
      ensure
        # Repair the topology so the looping republish thread can finish.
        http.client.delete_exchange(http.vhost, delivery_exchange)
        second.join(10)
      end
    end
  end

  describe 'Handler#error retry of a message that arrived through the leveled topology (#13)' do
    let(:levels) { 20 }
    let(:leveled) { leveled_publisher(levels: levels) }
    let(:legacy) do
      AdvancedSneakersActiveJob::DelayedPublisher.new(exchange: 'activejob', connection: connection).tap do |publisher|
        allow(publisher).to receive(:logger).and_return(logger)
      end
    end
    let(:consumer_channel) { connection.create_channel }
    let(:handler) { AdvancedSneakersActiveJob::Handler.new(consumer_channel, nil, {}) }
    let(:error) { StandardError.new('boom') }
    let(:fake_delivery_info) { Struct.new(:routing_key, :exchange, :delivery_tag, :consumer) }
    let(:fake_consumer) { Struct.new(:queue) }

    before do
      allow(AdvancedSneakersActiveJob).to receive(:leveled_delayed_publisher).and_return(leveled)
      allow(AdvancedSneakersActiveJob).to receive(:delayed_publisher).and_return(legacy)

      with_channel do |channel|
        leveled.declare_topology!(channel)
        queue = channel.queue('custom', durable: true)
        queue.bind(channel.direct('activejob', durable: true), routing_key: 'custom')
        queue.bind(delivery_exchange, routing_key: '#.custom') # what SneakersQueuePatch#subscribe does
      end
    end

    # basic.get the next message from "custom" and wrap its delivery info the
    # way a Sneakers consumer would hand it to the handler.
    def next_delivery(timeout: 6)
      queue = consumer_channel.queue('custom', durable: true)
      delivery = nil
      wait_until(timeout: timeout) { (delivery = queue.pop(manual_ack: true)).first }
      info, properties, payload = delivery
      raise 'no message arrived in "custom"' unless info

      [fake_delivery_info.new(info.routing_key, info.exchange, info.delivery_tag, fake_consumer.new(queue)), properties, payload]
    end

    it 'keeps retrying under :leveled without growing the routing key past the AMQP limit',
       with_config: { delayed_delivery: :leveled, retry_delay_proc: ->(_count) { 1 } } do
      leveled.publish('payload', routing_key: 'custom', headers: { 'delay' => 1 })

      key_lengths = []
      8.times do
        info, properties, payload = next_delivery
        key_lengths << info.routing_key.bytesize

        expect { handler.error(info, properties, payload, error) }
          .not_to raise_error, "routing key lengths per delivery: #{key_lengths.inspect}"
      end

      expect(key_lengths.uniq).to eq([levels * 2 + 'custom'.bytesize])
    end

    it 'counts the consumer-queue death across leveled retries (x-death-count survives dead-lettering)',
       with_config: { delayed_delivery: :leveled, retry_delay_proc: ->(_count) { 1 } } do
      leveled.publish('payload', routing_key: 'custom', headers: { 'delay' => 1 })

      # Read the count before #error: the handler mutates the received headers in place.
      counts = Array.new(4) do |attempt|
        info, properties, payload = next_delivery
        count = properties[:headers]['x-death-count']
        attempt < 3 ? handler.error(info, properties, payload, error) : consumer_channel.reject(info.delivery_tag)
        count
      end

      expect(counts).to eq([nil, 1, 2, 3])
    end

    it 'delivers the retry when the callable flips to :legacy between enqueue and retry',
       with_config: { delayed_delivery: -> { :legacy }, retry_delay_proc: ->(_count) { 1 } } do
      leveled.publish('payload', routing_key: 'custom', headers: { 'delay' => 1 })
      info, properties, payload = next_delivery
      handler.error(info, properties, payload, error)

      expect(wait_until(timeout: 4) { message_count('custom') == 1 }).to be(true),
                                                                         "retry routed with key #{info.routing_key.inspect} never arrived"
    end
  end

  describe 'parking queue and the alternate-exchange policy (#11)' do
    let(:leveled) { leveled_publisher(levels: 1) }

    before do
      with_channel { |channel| leveled.declare_topology!(channel) }
    end

    def set_policy(name, pattern:, definition:, priority: 0, apply_to: 'exchanges')
      http.client.update_policies_of(
        http.vhost, name,
        pattern: pattern, definition: definition, priority: priority, 'apply-to' => apply_to
      )
    end

    def publish_unroutable
      leveled.publish('payload', routing_key: 'nobody_home', headers: { 'delay' => 1 })
    end

    it 'parks a matured message that has no binding when the policy is set' do
      set_policy('delay-delivery-ae', pattern: '^delay\.delivery\.x$', definition: { 'alternate-exchange' => 'delay.delivery.unrouted.x' })

      publish_unroutable

      expect(wait_until(timeout: 5) { message_count('delay.delivery.parking') == 1 }).to be(true)
    end

    it 'drops the matured message when the policy is missing' do
      publish_unroutable
      sleep 3

      expect(message_count('delay.delivery.parking')).to eq(0)
    end

    it 'drops the matured message when a higher-priority policy also matches delay.delivery.x' do
      set_policy('delay-delivery-ae', pattern: '^delay\.delivery\.x$', definition: { 'alternate-exchange' => 'delay.delivery.unrouted.x' })
      set_policy('catch-all', pattern: '.*', definition: { 'max-length' => 1_000_000 }, priority: 1, apply_to: 'all')

      publish_unroutable
      sleep 3

      expect(message_count('delay.delivery.parking')).to eq(0)
    end
  end

  describe 'just-in-time destination binding under concurrency (#12)' do
    let(:leveled) { leveled_publisher(levels: 4) }

    before do
      with_channel do |channel|
        leveled.declare_topology!(channel)
        channel.queue('orders', durable: true)
      end
    end

    it 'binds once and publishes every message when many threads publish to a cold destination' do
      threads = Array.new(16) do |i|
        Thread.new { leveled.publish("payload-#{i}", routing_key: 'orders', headers: { 'delay' => 1 }) }
      end
      threads.each { |thread| expect(thread.join(10)).not_to be_nil }

      bindings = rabbitmq_bindings(queue: 'orders', exchange: delivery_exchange).select { |b| b.routing_key == '#.orders' }
      expect(bindings.size).to eq(1)
      expect(leveled.send(:destination_bound?, 'orders')).to be(true)
      expect(wait_until(timeout: 5) { message_count('orders') == 16 }).to be(true)
    end

    context 'when many threads publish to a missing destination (404 on each bind)' do
      let(:connection) { Bunny.new(ENV.fetch('RABBITMQ_URL'), continuation_timeout: 2_000).start }

      it 'publishes each message promptly' do
        pending 'defect: the 404 closes the bind channel from the broker side; Bunny reader thread then waits for ' \
                '@channel_mutex, which another thread holds in create_channel while it waits for channel.open-ok ' \
                'that only the reader can deliver, so both stall until continuation_timeout'

        durations = Array.new(4) do |i|
          Thread.new do
            started = Process.clock_gettime(Process::CLOCK_MONOTONIC)
            2.times { |j| leveled.publish("payload-#{i}-#{j}", routing_key: 'never_declared', headers: { 'delay' => 1 }) }
            Process.clock_gettime(Process::CLOCK_MONOTONIC) - started
          end
        end.map { |thread| thread.join(120).value.round(2) }

        expect(leveled.send(:destination_bound?, 'never_declared')).to be(false)
        expect(durations.max).to be < 1, "per-thread durations for 2 publishes each: #{durations.inspect}s"
      end
    end

    it 'does not deadlock in a child forked while another thread holds the memo mutex' do
      leveled.publish('warm', routing_key: 'orders', headers: { 'delay' => 1 })

      mutex = leveled.send(:bound_destinations_mutex)
      locked = Queue.new
      holder = Thread.new { mutex.synchronize { locked << true; sleep 2 } }
      locked.pop

      pid = fork do
        result = Timeout.timeout(1) { leveled.send(:destination_bound?, 'orders') }
        exit!(result ? 0 : 1)
      rescue Timeout::Error
        exit!(2)
      end
      _, status = Process.wait2(pid)
      holder.join

      expect(status.exitstatus).to eq(0)
    end
  end

  describe 'just-in-time destination binding when the bind channel breaks (#12)' do
    let(:publisher) { leveled_publisher(levels: 4) }
    let(:publish_exchange) { instance_double('Bunny::Exchange', publish: nil) }
    let(:publish_channel) { instance_double('Bunny::Channel', topic: publish_exchange) }
    let(:bind_channel) { instance_double('Bunny::Channel', open?: true) }
    let(:fake_connection) { instance_double('Bunny::Session', create_channel: bind_channel) }

    before do
      allow(publisher).to receive(:ensure_connection!)
      allow(publisher).to receive(:channel).and_return(publish_channel)
      allow(publisher).to receive(:connection).and_return(fake_connection)
    end

    it 'still publishes when the bind times out and closing the channel also fails' do
      allow(bind_channel).to receive(:queue_bind).and_raise(Timeout::Error)
      allow(bind_channel).to receive(:close).and_raise(Bunny::ConnectionClosedError.new(bind_channel))

      expect { publisher.publish('payload', routing_key: 'orders', headers: { 'delay' => 3 }) }.not_to raise_error

      expect(publish_exchange).to have_received(:publish)
      expect(publisher.send(:destination_bound?, 'orders')).to be(false)
    end
  end
end
