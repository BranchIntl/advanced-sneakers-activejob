# frozen_string_literal: true

require 'active_job/base'

describe AdvancedSneakersActiveJob::LeveledDelayedPublisher do
  let(:publisher) do
    # Skip BunnyPublisher::Base#initialize so unit tests don't need a live broker.
    publisher = described_class.allocate
    publisher.instance_variable_set(:@dlx_exchange_name, 'activejob')
    publisher
  end

  describe 'constants' do
    it 'declares 20 levels' do
      expect(described_class::LEVELS).to eq(20)
    end

    it 'caps max delay at 2^LEVELS - 1 seconds (~12.1 days)' do
      expect(described_class::MAX_DELAY).to eq((1 << 20) - 1)
      expect(described_class::MAX_DELAY).to eq(1_048_575)
    end

    it 'names the delivery exchange explicitly' do
      expect(described_class::DELIVERY_EXCHANGE).to eq('delay.delivery.x')
    end
  end

  describe '#level_queue_name' do
    it 'pads with leading zero to two digits' do
      expect(publisher.level_queue_name(0)).to eq('delay.level.00')
      expect(publisher.level_queue_name(5)).to eq('delay.level.05')
      expect(publisher.level_queue_name(19)).to eq('delay.level.19')
    end
  end

  describe '#level_exchange_name' do
    it 'pads with leading zero to two digits and appends .x' do
      expect(publisher.level_exchange_name(0)).to eq('delay.level.00.x')
      expect(publisher.level_exchange_name(5)).to eq('delay.level.05.x')
      expect(publisher.level_exchange_name(19)).to eq('delay.level.19.x')
    end
  end

  describe '#build_routing_key' do
    it 'encodes a delay of 1 as a single set bit in segment b00' do
      key = publisher.build_routing_key(1, 'destination')

      expect(key).to eq('0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.1.destination')
    end

    it 'encodes a delay of 2 as a single set bit in segment b01' do
      key = publisher.build_routing_key(2, 'destination')

      expect(key).to eq('0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.1.0.destination')
    end

    it 'encodes the worked example from the design doc (delay 47)' do
      # 47 = 0b101111 → bits 5, 3, 2, 1, 0 set
      key = publisher.build_routing_key(47, 'sla_ten_second')

      expect(key).to eq('0.0.0.0.0.0.0.0.0.0.0.0.0.0.1.0.1.1.1.1.sla_ten_second')
    end

    it 'encodes the maximum representable delay as all bits set' do
      key = publisher.build_routing_key(described_class::MAX_DELAY, 'q')

      bits = key.split('.').first(20).join
      expect(bits).to eq('1' * 20)
    end

    it 'always emits exactly LEVELS + 1 segments' do
      key = publisher.build_routing_key(47, 'sla_ten_second')

      expect(key.split('.').length).to eq(described_class::LEVELS + 1)
    end
  end

  describe '#publish' do
    let(:exchange) { instance_double('Bunny::Exchange', publish: nil) }
    let(:channel) { instance_double('Bunny::Channel') }

    before do
      allow(publisher).to receive(:channel).and_return(channel)
      allow(channel).to receive(:topic).and_return(exchange)
    end

    context 'when delay exceeds MAX_DELAY' do
      it 'raises DelayTooLargeError without publishing' do
        expect do
          publisher.publish('payload', routing_key: 'q', headers: { 'delay' => described_class::MAX_DELAY + 1 })
        end.to raise_error(AdvancedSneakersActiveJob::DelayTooLargeError, /exceeds.*max/)

        expect(exchange).not_to have_received(:publish)
      end
    end

    context 'when delay is positive and within bounds' do
      it 'targets the level exchange matching the highest set bit' do
        # 47 = 0b101111 -> highest bit is 5
        publisher.publish('payload', routing_key: 'sla_ten_second', headers: { 'delay' => 47 })

        expect(channel).to have_received(:topic).with('delay.level.05.x', durable: true)
        expect(exchange).to have_received(:publish).with(
          'payload',
          hash_including(routing_key: '0.0.0.0.0.0.0.0.0.0.0.0.0.0.1.0.1.1.1.1.sla_ten_second')
        )
      end

      it 'targets level.00 for delay of 1 second' do
        publisher.publish('payload', routing_key: 'q', headers: { 'delay' => 1 })

        expect(channel).to have_received(:topic).with('delay.level.00.x', durable: true)
      end

      it 'targets level.19 for delays at the upper bound' do
        publisher.publish('payload', routing_key: 'q', headers: { 'delay' => described_class::MAX_DELAY })

        expect(channel).to have_received(:topic).with('delay.level.19.x', durable: true)
      end

      it 'preserves caller-supplied options other than routing_key' do
        publisher.publish('payload',
                         routing_key: 'q',
                         headers: { 'delay' => 1, 'custom' => 'hdr' },
                         priority: 5)

        expect(exchange).to have_received(:publish).with(
          'payload',
          hash_including(headers: { 'delay' => 1, 'custom' => 'hdr' }, priority: 5)
        )
      end
    end

    context 'when delay is zero or negative (defensive path)' do
      let(:direct_exchange) { instance_double('Bunny::Exchange', publish: nil) }

      before do
        allow(channel).to receive(:direct).and_return(direct_exchange)
      end

      it 'falls through to the work exchange for delay 0' do
        publisher.publish('payload', routing_key: 'q', headers: { 'delay' => 0 })

        expect(channel).to have_received(:direct).with('activejob', durable: true)
        expect(direct_exchange).to have_received(:publish).with('payload', anything)
      end

      it 'falls through to the work exchange for negative delay' do
        publisher.publish('payload', routing_key: 'q', headers: { 'delay' => -5 })

        expect(channel).to have_received(:direct).with('activejob', durable: true)
      end
    end
  end

  describe '#declare_topology!' do
    let(:channel) { instance_double('Bunny::Channel') }
    let(:exchanges) { Hash.new { |h, k| h[k] = instance_double('Bunny::Exchange', bind: nil) } }
    let(:queues) { Hash.new { |h, k| h[k] = instance_double('Bunny::Queue', bind: nil) } }

    before do
      allow(publisher).to receive(:channel).and_return(channel)
      allow(channel).to receive(:topic) { |name, **| exchanges[name] }
      allow(channel).to receive(:queue) { |name, **| queues[name] }
    end

    it 'declares the delivery exchange first so level 0 can DLX to it' do
      publisher.declare_topology!

      expect(channel).to have_received(:topic).with('delay.delivery.x', durable: true).at_least(:once)
    end

    it 'declares 20 level topic exchanges' do
      publisher.declare_topology!

      (0...20).each do |n|
        expect(channel).to have_received(:topic).with(format('delay.level.%02d.x', n), durable: true).at_least(:once)
      end
    end

    it 'declares 20 level queues with quorum type and per-level TTL' do
      publisher.declare_topology!

      (0...20).each do |n|
        expected_args = {
          'x-queue-type' => 'quorum',
          'x-message-ttl' => (1 << n) * 1000,
          'x-dead-letter-exchange' => n.zero? ? 'delay.delivery.x' : format('delay.level.%02d.x', n - 1)
        }
        expect(channel).to have_received(:queue).with(
          format('delay.level.%02d', n),
          durable: true,
          arguments: expected_args
        )
      end
    end

    it 'level 0 dead-letters to the delivery exchange' do
      publisher.declare_topology!

      expect(channel).to have_received(:queue).with(
        'delay.level.00',
        durable: true,
        arguments: hash_including('x-dead-letter-exchange' => 'delay.delivery.x')
      )
    end

    it 'levels 1..19 dead-letter to the next-lower level exchange' do
      publisher.declare_topology!

      (1...20).each do |n|
        expect(channel).to have_received(:queue).with(
          format('delay.level.%02d', n),
          durable: true,
          arguments: hash_including('x-dead-letter-exchange' => format('delay.level.%02d.x', n - 1))
        )
      end
    end

    it 'binds each level queue with a "bit=1" pattern targeting its slot' do
      publisher.declare_topology!

      # Level 5: segment for bit 5 is at position LEVELS - 1 - 5 = 14
      level_5_queue = queues['delay.level.05']
      expected_pattern = '*.' * 14 + '1.' + '*.' * 5 + '#'
      # Normalize: split on '.' and re-join to compare cleanly.
      expect(level_5_queue).to have_received(:bind).with(
        exchanges['delay.level.05.x'],
        routing_key: expected_pattern.sub(/\.$/, '')
      )
    end

    it 'binds each level exchange with a "bit=0" pattern forwarding to the next-lower exchange' do
      publisher.declare_topology!

      # Level 5 with bit=0 forwards to level 4 exchange. The "0" sits at
      # segment LEVELS - 1 - 5 = 14.
      next_lower = exchanges['delay.level.04.x']
      expected_pattern = '*.' * 14 + '0.' + '*.' * 5 + '#'
      expect(next_lower).to have_received(:bind).with(
        exchanges['delay.level.05.x'],
        routing_key: expected_pattern.sub(/\.$/, '')
      )
    end

    it 'level 0 with bit=0 forwards directly to the delivery exchange' do
      publisher.declare_topology!

      delivery = exchanges['delay.delivery.x']
      # Segment for bit 0 sits at position LEVELS - 1 - 0 = 19.
      expected_pattern = '*.' * 19 + '0.#'
      expect(delivery).to have_received(:bind).with(
        exchanges['delay.level.00.x'],
        routing_key: expected_pattern
      )
    end

    it 'is idempotent (safe to call twice)' do
      publisher.declare_topology!
      expect { publisher.declare_topology! }.not_to raise_error
    end
  end
end
