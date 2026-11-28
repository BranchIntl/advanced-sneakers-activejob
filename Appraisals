# frozen_string_literal: true

appraise 'activejob-7.1.x' do
  gem 'rails', '~> 7.1.0'
  # ActiveSupport 7.1 passes quirks_mode to JSON.generate and JSON.parse; json 3 rejects that keyword.
  gem 'json', '< 3'
end

appraise 'activejob-8.1.x' do
  gem 'rails', '~> 8.1.0'
end
