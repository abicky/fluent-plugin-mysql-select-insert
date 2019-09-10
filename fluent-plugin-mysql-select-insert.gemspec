lib = File.expand_path("../lib", __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)

Gem::Specification.new do |spec|
  spec.name          = "fluent-plugin-mysql-select-insert"
  spec.version       = "0.1.0"
  spec.authors       = ["abicky"]
  spec.email         = ["takeshi.arabiki@gmail.com"]

  spec.summary       = %q{Fluentd output plugin to insert records by SELECT query.}
  spec.description   = %q{You can select records using events data and join multiple tables.}
  spec.homepage      = "https://github.com/abicky/fluent-plugin-mysql-select-insert"
  spec.license       = "Apache-2.0"

  test_files, files  = `git ls-files -z`.split("\x0").partition do |f|
    f.match(%r{^(test|spec|features)/})
  end
  spec.files         = files
  spec.executables   = files.grep(%r{^bin/}) { |f| File.basename(f) }
  spec.test_files    = test_files
  spec.require_paths = ["lib"]

  spec.add_development_dependency "bundler", ">= 1.16", "< 3"
  spec.add_development_dependency "rake", "~> 12.0"
  spec.add_development_dependency "test-unit", "~> 3.0"
  spec.add_runtime_dependency "fluentd", [">= 1.2.0", "< 2"]
  spec.add_runtime_dependency "mysql2-cs-bind"
end
