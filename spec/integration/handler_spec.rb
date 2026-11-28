# frozen_string_literal: true

describe 'Handler', :rabbitmq do
  before { cleanup_logs }

  context 'when job is not failing' do
    subject do
      start_sneakers_consumers(adapter: :advanced_sneakers)
      in_app_process(adapter: :advanced_sneakers) { CustomQueueJob.perform_later('good job') }
    end

    it 'does not handle job retries' do
      subject

      expect_logs name: 'rails',
                  to_include: [
                    'Performing CustomQueueJob from AdvancedSneakers(custom) with arguments: "good job"',
                    'Performed CustomQueueJob from AdvancedSneakers(custom)'
                  ],
                  to_exclude: 'Creating delayed queue'
    end
  end

  context 'when job is failing' do
    context 'when failure is handled by ActiveJob' do
      subject do
        in_app_process(adapter: :advanced_sneakers) do
          CustomQueueJob.discard_on 'StandardError'

          require 'rake'
          require 'sneakers/tasks'
          Rake::Task['sneakers:run'].invoke
        end

        in_app_process(adapter: :advanced_sneakers) { CustomQueueJob.perform_later('failing job') }
      end

      it 'does not handle job retries' do
        subject

        expect_logs name: 'rails',
                    to_include: [
                      'Performing CustomQueueJob from AdvancedSneakers(custom) with arguments: "failing job"',
                      'Discarded CustomQueueJob due to a StandardError'
                    ],
                    to_exclude: 'Creating delayed queue'
      end
    end

    context 'when failure is not handled by ActiveJob' do
      subject do
        start_sneakers_consumers(adapter: :advanced_sneakers)
        in_app_process(adapter: :advanced_sneakers) { CustomQueueJob.perform_later('failing job') }
      end

      it 'handles job retries' do
        subject

        expect_logs name: 'rails',
                    to_include: [
                      'Performing CustomQueueJob from AdvancedSneakers(custom) with arguments: "failing job"',
                      'to [activejob-delayed] with routing_key [custom] and delay [3]',
                      'Creating delayed queue'
                    ],
                    to_exclude: 'Performed CustomQueueJob from AdvancedSneakers(custom)'
      end

      it 'retries job with exponential backoff' do
        subject

        expect_delayed_queues(['delayed:3'])

        rabbitmq_messages('delayed:3', ackmode: 'reject_requeue_false') # simulate delayed message timeout

        expect_delayed_queues(['delayed:3', 'delayed:30'])

        rabbitmq_messages('delayed:30', ackmode: 'reject_requeue_false') # simulate delayed message timeout

        expect_delayed_queues(['delayed:3', 'delayed:30', 'delayed:90'])
      end

      # The consumer process declares each delayed queue on its own schedule; a fixed sleep is too short on slow CI runners.
      def expect_delayed_queues(expected, wait: 5)
        Timeout.timeout(wait) { sleep 0.1 until delayed_queues == expected }
      rescue Timeout::Error
        nil
      ensure
        expect(delayed_queues).to eq(expected)
      end

      def delayed_queues
        rabbitmq_queues(columns: [:name]).select { |queue| queue.name.starts_with?('delayed:') }.map(&:name)
      end

      describe 'retried job headers' do
        subject do
          super()
          sleep 0.1
          rabbitmq_messages('delayed:3').first.properties.headers
        end

        it 'have error details', :aggregate_failures do
          expect(subject['x-last-error-name']).to eq 'StandardError'
          expect(ActiveSupport::Gzip.decompress(Base64.decode64(subject['x-last-error-details']))).to include('Some error message')
        end
      end
    end
  end
end
