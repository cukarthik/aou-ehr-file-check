import glob

import settings
import os
import codecs
import pandas as pd
import numpy as np
import csv
# from csv_info import CsvInfo
import json

RESULT_SUCCESS = 'success'
# FILENAME_RE = re.compile('(person|visit_occurrence|condition_occurrence|procedure_occurrence|drug_exposure|measurement)\.csv')
# FILENAME_FORMAT = '<table>.csv'
MSG_CANNOT_PARSE_FILENAME = 'Cannot parse filename'
MSG_INVALID_TYPE = 'Type mismatch'

HEADER_KEYS = ['filename', 'table_name']
ERROR_KEYS = ['message', 'column_name', 'actual', 'expected']


def get_cdm_table_columns(table_name):
    # allow files to be found regardless of CaSe
    file=os.path.join(settings.cdm_metadata_path, table_name.lower()+'.json')
    if os.path.isfile(file):
        return json.load(open(file))
    else:
        return None



def type_eq(cdm_column_type, submission_column_type):
    """
    Compare column type in spec with column type in submission
    :param cdm_column_type:
    :param submission_column_type:
    :return:
    """
    if cdm_column_type == 'time':
        return submission_column_type == 'character varying'
    if cdm_column_type == 'integer':
        return submission_column_type == 'int'
    if cdm_column_type in ('character varying', 'text', 'string'):
        return submission_column_type in ('str', 'unicode', 'object')
    if cdm_column_type == 'date':
        return submission_column_type in ('str', 'unicode', 'date')
    if cdm_column_type == 'numeric':
        return submission_column_type == 'float'
    raise Exception('Unsupported CDM column type ' + cdm_column_type)

# code from: http://stackoverflow.com/questions/2456380/utf-8-html-and-css-files-with-bom-and-how-to-remove-the-bom-with-python
def remove_bom(filename):
    if os.path.isfile(filename):
        f = open(filename, 'rb')

        # read first 4 bytes
        header = f.read(4)

        # check for BOM
        bom_len = 0
        encodings = [(codecs.BOM_UTF32, 4),
                     (codecs.BOM_UTF16, 2),
                     (codecs.BOM_UTF8, 3)]

        # remove appropriate number of bytes
        for h, l in encodings:
            if header.startswith(h):
                bom_len = l
                break
        f.seek(0)
        f.read(bom_len)
        return f

def process_file(file_path):
    """
    Find sprint files for the specified HPO and load CDM tables in the schema
    :return:
    """
    # table_map = dict()

    filename, file_extension = os.path.splitext(file_path)
    file_path_parts = filename.split(os.sep)
    table_name = file_path_parts[-1]

    #get the column definitions for a particular OMOP table
    cdm_table_columns = get_cdm_table_columns(table_name)
    # all_meta_items = cdm_table_columns.to_rows()

    #ToDo: may not need these lines b/c all tables are required and we will evaluate only files submitted for processing
    # included_tables = pd.read_csv(settings.pmi_tables_csv_path).table_name.unique()
    # tables = cdm_df[cdm_df['table_name'].isin(included_tables)].groupby(['table_name'])

    phase = 'Received CSV file "%s"' % table_name

    result = {'passed': False, 'errors': []}
    result['filename'] = table_name+file_extension

    if cdm_table_columns is None:
        result['errors'].append(dict(message='File is not a OMOP CDM table: %s' % table_name))
    else:

        try:
            # get column names for this table
            column_names = [col['name'] for col in cdm_table_columns]
            csv_columns = list(pd.read_csv(remove_bom(file_path), nrows=1).columns.values)
            datetime_columns = [col_name.lower() for col_name in csv_columns if 'date' in col_name.lower()]

            phase = 'Parsing CSV file %s' % file_path
            # read file to be processed
            df = pd.read_csv(remove_bom(file_path), na_values=['', ' ', '.'], parse_dates=datetime_columns,
                             infer_datetime_format=True)
            print(phase)

            # lowercase field names
            df = df.rename(columns=str.lower)

            # add missing columns (with NaN values)
            # df = df.reindex(columns=column_names)

            # fill in blank concept_id columns with 0
            # concept_columns = [col_name for col_name in column_names if col_name.endswith('concept_id') and 'source' not in col_name]
            # df[concept_columns] = df[concept_columns].fillna(value=0)


        # CSV parser is flexible/lenient, but we can only support proper comma-delimited files
        # with open(file_path) as input_file:
        #     data_file = CsvInfo(input_file, table_name)

            # get table metadata
            # meta_items = filter(lambda r: r[0] == table_name, all_meta_items)

            # Check each column exists with correct type and required
            for meta_item in cdm_table_columns:
                meta_column_name = meta_item['name']
                meta_column_required =  meta_item['mode']=='required'
                meta_column_type = meta_item['type']
                submission_has_column = False

                for submission_column in df.columns:
                    if submission_column == meta_column_name:
                        submission_has_column = True
                        submission_column_type = df[submission_column].dtype

                        # If all empty don't do type check
                        if submission_column_type != None:
                            if not type_eq(meta_column_type, submission_column_type):
                                e = dict(message=MSG_INVALID_TYPE,
                                         column_name=submission_column,
                                         actual=submission_column_type,
                                         expected=meta_column_type)
                                result['errors'].append(e)

                        # Check if any nulls present in a required field
                        if meta_column_required and df[submission_column].isnull().sum()>0:#submission_column['stats']['nulls']:
                            result['errors'].append(dict(message='NULL values are not allowed for column',
                                                         column_name=submission_column))
                        continue

                #Check if the column is required
                if not submission_has_column and meta_column_required:
                    result['errors'].append(dict(message='Missing required column', column_name=meta_column_name))

        except Exception as e:
            print(e)

    return result

def evaluate_submission(d):
    out_dir = os.path.join(d, 'errors')
    if not os.path.exists(out_dir):
        os.makedirs(out_dir)

    output_filename = os.path.join(out_dir, 'results.csv')
    with open(output_filename, 'w') as out:
        #Create header information for results file
        field_names = HEADER_KEYS + ERROR_KEYS
        writer = csv.DictWriter(out, fieldnames=field_names, lineterminator='\n', quoting=csv.QUOTE_ALL)
        writer.writeheader()

        for f in glob.glob(os.path.join(d, '*.csv')):
            file_path_parts = f.split(os.sep)
            filename = file_path_parts[-1]
            # output_filename = os.path.join(out_dir, filename+'_error')

            result = process_file(f)
            rows = []
            for error in result['errors']:
                row = dict()
                for header_key in HEADER_KEYS:
                    row[header_key] = result.get(header_key)
                for error_key in ERROR_KEYS:
                    row[error_key] = error.get(error_key)
                rows.append(row)

            if len(rows) > 0:
                writer.writerows(rows)
            # out.writelines(rows)


if __name__ == '__main__':
    evaluate_submission(settings.csv_dir)
    # process_file('/Users/karthik/Dev/projects/Columbia/PMI/aou-ehr-file-check/examples/person.csv')
