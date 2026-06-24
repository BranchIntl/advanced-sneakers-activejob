# frozen_string_literal: true

module AdvancedSneakersActiveJob
  # Bounded power-of-two TTL delayed publisher.
  #
  # 20 quorum queues, one per power-of-2 second interval. A delay of D
  # seconds routes through the levels matching its set bits, summing
  # TTL to D exactly. Pattern follows NServiceBus / Celery.
  #
  # Implementation note: parent's publish lifecycle is inherited as-is.
  # We do NOT override BunnyPublisher::Base#publish. Instead we override
  # two hooks the parent already exposes:
  #
  #   #exchange         - parent reads this once per publish to get the
  #                       target exchange. We compute the right level
  #                       exchange (or the direct delivery exchange for
  #                       delay <= 0) from the current @message_options,
  #                       which the parent sets inside its own mutex
  #                       before calling us.
  #
  #   #reset_exchange!  - parent calls this from with_errors_handling when
  #                       a Bunny::ChannelAlreadyClosed forces a channel
  #                       rebuild. We invalidate our memoized level
  #                       exchange handles so the retry hits the new
  #                       channel.
  #
  # The only thing we add at the top of #publish is a pre-step: validate
  # the delay against MAX_DELAY (raises before mutex), and rewrite the
  # caller's routing_key into the 21-segment binary-decomposed form the
  # level topic exchanges expect. Then we hand off to super, which runs
  # the parent's full publish flow (mutex, ensure_connection,
  # with_errors_handling, callbacks, exchange.publish).
  #
  # This inherits, for free:
  #
  #   * Bunny-channel thread-safety (@mutex.synchronize wraps everything).
  #   * Lazy connection + channel open (ensure_connection!).
  #   * Connection recovery and channel rebuild on transient broker errors
  #     (with_errors_handling retries on Bunny::ChannelAlreadyClosed,
  #     Bunny::ConnectionClosedError, Bunny::NetworkFailure,
  #     Bunny::ConnectionLevelException, Timeout::Error).
  #   * Per-publish callbacks (run_callbacks(:publish)).
  #
  # The previous implementation overrode #publish and replicated mutex +
  # ensure_connection! manually, leaving with_errors_handling and the
  # callback wrap as gaps tracked in BEP-9829. This refactor closes those
  # gaps by inheriting the contracts rather than re-implementing them.
  class LeveledDelayedPublisher < ::BunnyPublisher::Base
    # Level N has TTL 2^N seconds. 20 levels covers ~12.1 days.
    LEVELS = 20

    # Delays above this raise DelayTooLargeError. No silent capping.
    MAX_DELAY = (1 << LEVELS) - 1

    # Every worker queue binds to this with pattern "#.<queue_name>".
    DELIVERY_EXCHANGE = 'delay.delivery.x'

    delegate :logger, to: :'::ActiveJob::Base'

    attr_reader :dlx_exchange_name

    def initialize(exchange:, **options)
      # Base needs an exchange; we route per-publish so the parent's
      # @exchange is effectively a placeholder. Our #exchange override
      # picks the real per-message target.
      super(**options.merge(
        exchange: DELIVERY_EXCHANGE,
        exchange_options: { type: 'topic', durable: true }
      ))
      @dlx_exchange_name = exchange
    end

    # Declare the 20-level topology. Idempotent. Call at boot before
    # any worker queue binds to DELIVERY_EXCHANGE.
    #
    # Accepts an optional channel override. At boot time the publisher's own
    # channel may not be open yet (BunnyPublisher::Base opens lazily on first
    # publish), so host apps should pass in an already-open channel from the
    # same connection used to declare their other queues.
    def declare_topology!(channel_override = nil)
      ch = channel_override || channel
      raise 'LeveledDelayedPublisher#declare_topology! requires an open channel' if ch.nil?

      ch.topic(DELIVERY_EXCHANGE, durable: true)

      (0...LEVELS).each do |n|
        level_exchange = ch.topic(level_exchange_name(n), durable: true)
        next_dlx_name  = n.zero? ? DELIVERY_EXCHANGE : level_exchange_name(n - 1)

        level_queue = ch.queue(
          level_queue_name(n),
          durable: true,
          arguments: {
            'x-queue-type'           => 'quorum',
            'x-message-ttl'          => (1 << n) * 1000,
            'x-dead-letter-exchange' => next_dlx_name
          }
        )

        # Bit N = 1: land in this level's queue.
        level_queue.bind(level_exchange, routing_key: bit_pattern(n, '1'))

        # Bit N = 0: forward straight to next-lower exchange.
        next_exchange = ch.topic(next_dlx_name, durable: true)
        next_exchange.bind(level_exchange, routing_key: bit_pattern(n, '0'))
      end

      logger.info { "LeveledDelayedPublisher: topology declared (#{LEVELS} levels, max #{MAX_DELAY}s)" } if defined?(::Rails)

      nil
    end

    # Adapter calls this with routing_key=<destination> and headers={'delay'=>N}.
    # We only do pre-publish work here (validation + routing-key rewrite),
    # then hand off to the parent's full publish lifecycle via super.
    def publish(message, options = {})
      delay = options.dig(:headers, 'delay').to_i

      if delay > MAX_DELAY
        raise DelayTooLargeError,
              "delay #{delay}s exceeds max #{MAX_DELAY}s (~#{MAX_DELAY / 86_400} days)"
      end

      if delay > 0
        destination = options[:routing_key].to_s
        options = options.merge(routing_key: build_routing_key(delay, destination))

        logger.debug do
          "LeveledDelayedPublisher: publishing to [#{level_exchange_name(highest_set_bit(delay))}] " \
          "with routing_key [#{options[:routing_key]}] and delay [#{delay}]"
        end
      end

      super(message, options)
    end

    # OVERRIDE: parent reads `exchange` once per publish to get the target.
    # We pick the right one based on the current message's delay header.
    #
    # @message_options is set by the parent inside its own @mutex.synchronize
    # block before this is called, so we can read it without additional
    # synchronization. For delay > 0 we return the level topic exchange
    # matching the highest set bit; for delay <= 0 we return a direct
    # exchange to the configured dlx_exchange_name for immediate delivery.
    def exchange
      delay = @message_options&.dig(:headers, 'delay').to_i

      if delay <= 0
        @immediate_exchange ||= channel.direct(dlx_exchange_name, durable: true)
      else
        level = highest_set_bit(delay)
        level_exchanges[level] ||= channel.topic(level_exchange_name(level), durable: true)
      end
    end

    # OVERRIDE: parent calls this from with_errors_handling when a
    # Bunny::ChannelAlreadyClosed forces a channel rebuild. The parent
    # rebuilds @channel and its own @exchange; we invalidate our memoized
    # per-level and immediate-direct handles so the retry hits the new
    # channel rather than the dead one.
    def reset_exchange!
      super
      @level_exchanges = nil
      @immediate_exchange = nil
    end

    # 21-segment routing key: b{LEVELS-1}...b00.<destination>
    def build_routing_key(delay, destination)
      bits = (LEVELS - 1).downto(0).map { |bit| (delay >> bit) & 1 }
      (bits + [destination]).join('.')
    end

    def level_queue_name(n)
      format('delay.level.%02d', n)
    end

    def level_exchange_name(n)
      format('delay.level.%02d.x', n)
    end

    private

    def level_exchanges
      @level_exchanges ||= Array.new(LEVELS)
    end

    # Topic pattern with slot N pinned, other bit slots wild, '#' for destination.
    # Slot for bit N is at position LEVELS-1-N (routing key is MSB-first).
    def bit_pattern(n, value)
      segments = Array.new(LEVELS, '*')
      segments[LEVELS - 1 - n] = value
      "#{segments.join('.')}.#"
    end

    # Integer log2 floor without Math.log2's float edge cases.
    def highest_set_bit(n)
      bit = -1
      remaining = n
      while remaining.positive?
        bit += 1
        remaining >>= 1
      end
      bit
    end
  end
end
