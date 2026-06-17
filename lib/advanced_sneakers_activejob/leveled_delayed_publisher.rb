# frozen_string_literal: true

module AdvancedSneakersActiveJob
  # Bounded power-of-two TTL delayed publisher.
  #
  # 20 quorum queues, one per power-of-2 second interval. A delay of D
  # seconds routes through the levels matching its set bits, summing
  # TTL to D exactly. Pattern follows NServiceBus / Celery.
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
      # Base needs an exchange; we route per-publish so pin to DELIVERY_EXCHANGE.
      super(**options.merge(
        exchange: DELIVERY_EXCHANGE,
        exchange_options: { type: 'topic', durable: true }
      ))
      @dlx_exchange_name = exchange
    end

    # Declare the 20-level topology. Idempotent. Call at boot before
    # any worker queue binds to DELIVERY_EXCHANGE.
    def declare_topology!
      ch = channel
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
    def publish(message, options = {})
      destination = options[:routing_key].to_s
      delay       = options.dig(:headers, 'delay').to_i

      if delay > MAX_DELAY
        raise DelayTooLargeError,
              "delay #{delay}s exceeds max #{MAX_DELAY}s (~#{MAX_DELAY / 86_400} days)"
      end

      return publish_immediately(message, options) if delay <= 0

      highest_bit    = highest_set_bit(delay)
      target_name    = level_exchange_name(highest_bit)
      routing_key    = build_routing_key(delay, destination)
      level_exchange = level_exchange_for(highest_bit)

      logger.debug do
        "LeveledDelayedPublisher: publishing to [#{target_name}] with routing_key [#{routing_key}] and delay [#{delay}]"
      end

      level_exchange.publish(message, options.merge(routing_key: routing_key))
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

    def level_exchange_for(n)
      level_exchanges[n] ||= channel.topic(level_exchange_name(n), durable: true)
    end

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

    # Defensive: adapter normally short-circuits delay <= 0 via enqueue().
    def publish_immediately(message, options)
      channel.direct(dlx_exchange_name, durable: true).publish(message, options)
    end
  end
end
