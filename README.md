# AoU EHR Submission Validator

Validate submissions for the All of Us data sprints

## Running
 * Update `_settings.py` and rename it to `settings.py`
 * Create a folder and place all the EHR submission files to be validated into it
 * Set the "csv_dir" parameter in `settings.py` to the full path of the folder created above
 * Execute the following at the command line:
 
        python omop_file_validator.py

## Validation logic
 * File names must follow naming convention `{table}.csv`
     * `table` an OMOP CDM table listed in [resources/omop](resources/omop)
 * Files must be in CSV format (comma-delimited) as specified by [rfc4180](https://tools.ietf.org/html/rfc4180)
 * Column names and types must follow the conventions in [resources/omop](resources/omop)
