# frozen_string_literal: true

# i18n 1.15.0 calls Fiber[:i18n_config] which requires Ruby 3.2+. Pin below
# that across the matrix so Ruby 3.1 jobs can still resolve dependencies.
I18N_RUBY_31_COMPAT = '< 1.15'

appraise 'activejob-6.0.x' do
  gem 'rails', '~> 6.0.3'
  gem 'concurrent-ruby', '1.3.4'
  gem 'i18n', I18N_RUBY_31_COMPAT
end

appraise 'activejob-6.1.x' do
  gem 'rails', '~> 6.1.0'
  gem 'concurrent-ruby', '1.3.4'
  gem 'i18n', I18N_RUBY_31_COMPAT
end

appraise 'activejob-7.0.x' do
  gem 'rails', '~> 7.0.0'
  gem 'concurrent-ruby', '1.3.4'
  gem 'i18n', I18N_RUBY_31_COMPAT
end

appraise 'activejob-7.1.x' do
  gem 'rails', '~> 7.1.0'
  gem 'i18n', I18N_RUBY_31_COMPAT
end
