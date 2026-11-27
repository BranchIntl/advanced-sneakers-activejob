# frozen_string_literal: true

require 'active_job'
require 'active_job/queue_adapters'

describe AdvancedSneakersActiveJob::Handler, 'x-death-count' do
  let(:channel) { instance_double('Bunny::Channel', acknowledge: nil) }
  let(:handler) { described_class.new(channel, 'custom', {}) }
  let(:consumer) { instance_double('Bunny::Consumer', queue: instance_double('Bunny::Queue', name: 'custom')) }
  let(:delivery_info) do
    instance_double('Bunny::DeliveryInfo', routing_key: 'custom', exchange: 'activejob', delivery_tag: 'tag-1', consumer: consumer)
  end
  let(:publisher) { instance_double(AdvancedSneakersActiveJob::DelayedPublisher, publish: nil) }

  before { allow(AdvancedSneakersActiveJob).to receive(:delayed_publisher).and_return(publisher) }

  it 'continues the count of an in-flight message that has only the old x-death row' do
    old_row = { 'count' => 2, 'reason' => 'rejected', 'queue' => 'custom', 'exchange' => 'activejob', 'routing-keys' => ['custom'] }

    handler.error(delivery_info, { headers: { 'x-death' => [old_row] } }, 'payload', StandardError.new('boom'))

    expect(publisher).to have_received(:publish).with(
      'payload',
      hash_including(headers: hash_including('x-death-count' => 3, 'delay' => AdvancedSneakersActiveJob::EXPONENTIAL_BACKOFF[3]))
    )
    expect(old_row['count']).to eq(3)
  end
end
