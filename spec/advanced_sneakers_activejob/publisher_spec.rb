# frozen_string_literal: true

require 'logger'

describe AdvancedSneakersActiveJob::Publisher do
  let(:delivery_exchange_name) { AdvancedSneakersActiveJob::LeveledDelayedPublisher::DELIVERY_EXCHANGE }

  describe '#declare_republish_queue_binding (Mandatory override)' do
    # Bypass BunnyPublisher::Base#initialize so unit tests don't need a broker.
    let(:publisher) do
      p = described_class.allocate
      allow(p).to receive(:logger).and_return(Logger.new(IO::NULL))
      p
    end

    let(:queue) { instance_double('Bunny::Queue', name: 'orders') }
    let(:bind_channel) { instance_double('Bunny::Channel', topic: nil, queue_bind: nil, open?: true, close: nil) }
    let(:connection) { instance_double('Bunny::Session', create_channel: bind_channel) }

    before do
      # super() chains into Mandatory#declare_republish_queue_binding, which binds
      # the queue to the publish exchange (activejob). Stub the super call so the
      # unit test isolates OUR added delivery-exchange bind; the activejob bind is
      # exercised by the real-broker test below.
      allow_any_instance_of(BunnyPublisher::Mandatory).to receive(:declare_republish_queue_binding)
      allow(publisher).to receive(:connection).and_return(connection)
    end

    it 'chains super (preserving the activejob bind) before adding the delivery bind' do
      expect_any_instance_of(BunnyPublisher::Mandatory).to receive(:declare_republish_queue_binding).with(queue)

      publisher.declare_republish_queue_binding(queue)
    end

    it 'idempotently declares delay.delivery.x as a durable topic exchange on its own channel' do
      publisher.declare_republish_queue_binding(queue)

      expect(bind_channel).to have_received(:topic).with(delivery_exchange_name, durable: true)
    end

    it 'binds the queue to delay.delivery.x with a "#.<queue_name>" routing key' do
      publisher.declare_republish_queue_binding(queue)

      expect(bind_channel).to have_received(:queue_bind).with('orders', delivery_exchange_name, routing_key: '#.orders')
    end

    it 'closes its own channel afterwards' do
      publisher.declare_republish_queue_binding(queue)

      expect(bind_channel).to have_received(:close)
    end

    context 'when the delivery-exchange bind fails' do
      let(:real_logger) { instance_double(Logger) }

      before do
        allow(publisher).to receive(:logger).and_return(real_logger)
        allow(real_logger).to receive(:error)
        # The broker closed the channel that issued the refused declare.
        allow(bind_channel).to receive(:open?).and_return(false)
        allow(bind_channel).to receive(:topic).and_raise(
          Bunny::PreconditionFailed.new('PRECONDITION_FAILED - inequivalent arg', bind_channel, false)
        )
      end

      it 'does not break the republish flow (swallows the error)' do
        expect { publisher.declare_republish_queue_binding(queue) }.not_to raise_error
      end

      it 'logs the failure at error level naming the queue and exchange' do
        publisher.declare_republish_queue_binding(queue)

        expect(real_logger).to have_received(:error) do |&block|
          msg = block.call
          expect(msg).to include('orders')
          expect(msg).to include(delivery_exchange_name)
        end
      end

      it 'still performed the activejob bind via super' do
        expect_any_instance_of(BunnyPublisher::Mandatory).to receive(:declare_republish_queue_binding).with(queue)

        publisher.declare_republish_queue_binding(queue)
      end
    end

    it 'swallows and logs a failure to close its channel' do
      real_logger = instance_double(Logger, error: nil)
      allow(publisher).to receive(:logger).and_return(real_logger)
      allow(bind_channel).to receive(:close).and_raise(Bunny::ConnectionClosedError.new(bind_channel))

      expect { publisher.declare_republish_queue_binding(queue) }.not_to raise_error
      expect(real_logger).to have_received(:error)
    end
  end

  describe 'unrouted immediate publish auto-binds both exchanges', :rabbitmq do
    let(:connection) { Bunny.new(ENV.fetch('RABBITMQ_URL')).start }
    let(:logger) { Logger.new(IO::NULL) }

    let(:publisher) do
      described_class.new(
        exchange: 'activejob',
        exchange_options: { type: 'direct', durable: true },
        connection: connection
      )
    end

    before do
      allow(publisher).to receive(:logger).and_return(logger)
      allow(AdvancedSneakersActiveJob.config).to receive(:handle_unrouted_messages).and_return(true)

      # The delivery exchange must exist as a durable topic for the extra bind to
      # succeed; declare it up front (idempotent with the override's own declare).
      setup = connection.create_channel
      setup.topic(delivery_exchange_name, durable: true)
      setup.close
    end

    after { connection.close }

    it 'auto-creates the queue bound to BOTH activejob and delay.delivery.x' do
      # No queue named "ghost_orders" exists, so this immediate publish is
      # unrouted; Mandatory catches the return and republishes after declaring
      # the queue + bindings. The republish happens on a background thread.
      publisher.publish('payload', routing_key: 'ghost_orders')

      wait_for do
        rabbitmq_bindings(queue: 'ghost_orders', exchange: 'activejob').any? &&
          rabbitmq_bindings(queue: 'ghost_orders', exchange: delivery_exchange_name).any?
      end

      activejob_keys = rabbitmq_bindings(queue: 'ghost_orders', exchange: 'activejob').map(&:routing_key)
      delivery_keys = rabbitmq_bindings(queue: 'ghost_orders', exchange: delivery_exchange_name).map(&:routing_key)

      expect(activejob_keys).to include('ghost_orders')
      expect(delivery_keys).to include('#.ghost_orders')
    end

    # Poll the broker until the async republish thread has done its work, or fail.
    def wait_for(timeout: 5)
      deadline = Time.now + timeout
      loop do
        return if yield
        raise "condition not met within #{timeout}s" if Time.now > deadline

        sleep 0.05
      end
    rescue Bunny::NotFound
      # queue/bindings not there yet; keep polling until the deadline.
      raise if Time.now > deadline

      sleep 0.05
      retry
    end
  end

  describe 'unrouted immediate publish when the user may not configure delay.delivery.x', :rabbitmq do
    let(:http) { RabbitmqHelpers.http_api }
    let(:limited_user) { 'asaj_limited' }
    let(:admin) { Bunny.new(ENV.fetch('RABBITMQ_URL')).start }
    let(:limited) do
      uri = URI(ENV.fetch('RABBITMQ_URL'))
      uri.user = limited_user
      uri.password = limited_user
      Bunny.new(uri.to_s).start
    end
    let(:logger) { Logger.new(IO::NULL) }

    let(:publisher) do
      described_class.new(exchange: 'activejob', exchange_options: { type: 'direct', durable: true }, connection: limited)
    end

    before do
      http.client.update_user(limited_user, password: limited_user, tags: '')
      http.client.update_permissions_of(http.vhost, limited_user,
                                        configure: '^(?!delay\\.delivery\\.x$).*', write: '.*', read: '.*')
      allow(publisher).to receive(:logger).and_return(logger)
      allow(logger).to receive(:error).and_call_original
      allow(AdvancedSneakersActiveJob.config).to receive(:handle_unrouted_messages).and_return(true)
    end

    after do
      limited.close if limited.open?
      admin.close if admin.open?
      http.client.delete_user(limited_user)
    end

    def ghost_orders_count
      channel = admin.create_channel
      channel.queue_declare('ghost_orders', passive: true).message_count
    rescue Bunny::NotFound
      0
    ensure
      channel.close if channel&.open?
    end

    it 'republishes once, logs one error and keeps publishing (403 on the delay.delivery.x declare)' do
      resets = 0
      allow(publisher).to receive(:reset_exchange!).and_wrap_original do |original|
        resets += 1
        original.call
      end

      publisher.publish('unrouted', routing_key: 'ghost_orders')
      sleep 0.5 # let the returned-message thread take @mutex

      second = Thread.new { publisher.publish('second', routing_key: 'ghost_orders') }
      expect(second.join(3)).not_to be_nil, "second publish still blocked after 3s; reset_exchange! ran #{resets} times"

      deadline = Time.now + 5
      sleep 0.05 until ghost_orders_count == 2 || Time.now > deadline

      expect(ghost_orders_count).to eq(2)
      expect(resets).to eq(0)
      expect(logger).to have_received(:error).once
      expect(rabbitmq_bindings(queue: 'ghost_orders', exchange: delivery_exchange_name)).to be_empty
    end
  end
end
