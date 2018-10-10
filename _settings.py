import inspect
import os

base_path = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
resource_path = os.path.join(base_path, 'resources')
test_path = os.path.join(base_path, 'tests')
test_resource_path = os.path.join(test_path, 'resources')
example_path = os.path.join(test_resource_path, 'examples_erroneous')
cdm_metadata_path = os.path.join(resource_path, 'omop')

# Configuration
csv_dir = 'path/to/csv_files'  # location of files to validate, evaluate
