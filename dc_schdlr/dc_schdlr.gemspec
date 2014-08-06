# coding: utf-8
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'dc_schdlr/version'

Gem::Specification.new do |spec|
  spec.name          = "dc_schdlr"
  spec.version       = DcSchdlr::VERSION
  spec.authors       = ["abhishek arora"]
  spec.email         = ["abhishenator@gmail.com"]
  spec.summary       = %q{Data Center Scheduler}
  spec.description   = %q{Data Center Scheduler}
  spec.homepage      = ""
  spec.license       = ""

  spec.files         = `git ls-files -z`.split("\x0")
  spec.executables   = spec.files.grep(%r{^bin/}) { |f| File.basename(f) }
  spec.test_files    = spec.files.grep(%r{^(test|spec|features)/})
  spec.require_paths = ["lib"]

  spec.add_development_dependency "bundler", "~> 1.5"
  spec.add_runtime_dependency 'PriorityQueue', '~> 0.1.2'
  spec.add_development_dependency "rake"
end
