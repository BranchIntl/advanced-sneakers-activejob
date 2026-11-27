# frozen_string_literal: true

appraise 'activejob-6.0.x' do
  gem 'rails', '~> 6.0.3'
  gem 'concurrent-ruby', '1.3.4'
end

appraise 'activejob-6.1.x' do
  gem 'rails', '~> 6.1.0'
  gem 'concurrent-ruby', '1.3.4'
end

appraise 'activejob-7.0.x' do
  gem 'rails', '~> 7.0.0'
  gem 'concurrent-ruby', '1.3.4'
end

appraise 'activejob-7.1.x' do
  gem 'rails', '~> 7.1.0'
  # irb pulls prism. Every precompiled prism 2.x gem needs Ruby 3.3+, yet older Bundler installs one on Ruby 2.7-3.2.
  gem 'prism', '< 2'
end

appraise 'activejob-8.1.x' do
  gem 'rails', '~> 8.1.0'
end
