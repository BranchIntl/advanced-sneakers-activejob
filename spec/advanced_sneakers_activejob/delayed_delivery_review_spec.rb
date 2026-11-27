# frozen_string_literal: true

require 'logger'
require 'active_job'
require 'active_job/queue_adapters'

# Repros and coverage for the v0.8.0 delayed-delivery changes (#11, #13),
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

    it 'logs one error and keeps the publisher usable for other publishes' do
      allow(logger).to receive(:error).and_call_original
      resets = 0
      allow(publisher).to receive(:reset_exchange!).and_wrap_original do |original|
        resets += 1
        original.call
      end

      publisher.publish('unrouted', routing_key: 'ghost_orders')
      sleep 0.5 # let the returned-message thread take @mutex

      second = Thread.new { publisher.publish('second', routing_key: 'ghost_orders') }
      expect(second.join(3)).not_to be_nil, "second publish still blocked after 3s; reset_exchange! ran #{resets} times"

      expect(wait_until(timeout: 5) { message_count('ghost_orders') == 2 }).to be(true)
      expect(resets).to eq(0)
      expect(logger).to have_received(:error).once
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
      pending 'defect: each retry prepends another 20 bit segments to the already bit-prefixed ' \
              'routing key; at > 255 bytes amq-protocol raises RangeError and the message is never acked'

      leveled.publish('payload', routing_key: 'custom', headers: { 'delay' => 1 })

      key_lengths = []
      8.times do
        info, properties, payload = next_delivery
        key_lengths << info.routing_key.bytesize

        expect { handler.error(info, properties, payload, error) }
          .not_to raise_error, "routing key lengths per delivery: #{key_lengths.inspect}"
      end
    end

    it 'counts the consumer-queue death across leveled retries (x-death survives dead-lettering)',
       with_config: { delayed_delivery: :leveled, retry_delay_proc: ->(_count) { 1 } } do
      if connection.server_properties['version'].to_i >= 4
        pending 'defect (predates #13): RabbitMQ 4.x drops client-set x-death rows on dead-lettering, so the ' \
                'handler always computes count 1 and retries every retry_delay_proc.call(1) seconds without backoff'
      end

      leveled.publish('payload', routing_key: 'custom', headers: { 'delay' => 1 })

      # Read the count before #error: the handler mutates the received headers in place.
      counts = Array.new(4) do |attempt|
        info, properties, payload = next_delivery
        count = properties[:headers]['x-death'].find { |death| death['queue'] == 'custom' }&.fetch('count')
        attempt < 3 ? handler.error(info, properties, payload, error) : consumer_channel.reject(info.delivery_tag)
        count
      end

      expect(counts).to eq([nil, 1, 2, 3])
    end

    it 'delivers the retry when the callable flips to :legacy between enqueue and retry',
       with_config: { delayed_delivery: -> { :legacy }, retry_delay_proc: ->(_count) { 1 } } do
      pending 'defect (predates #13, still reachable through a callable): the legacy retry ' \
              'dead-letters into the direct activejob exchange with the bit-prefixed key, which no queue binds'

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
end
