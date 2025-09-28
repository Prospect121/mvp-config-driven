import sys, yaml, jsonschema
schema = yaml.safe_load(open('ci/dataset_manifest_schema.yml'))
manifest = yaml.safe_load(open(sys.argv[1]))
jsonschema.validate(instance=manifest, schema=schema)
print('manifest OK')