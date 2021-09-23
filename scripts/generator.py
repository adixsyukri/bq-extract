from yaml import load
import argparse
import os
from pathlib import PurePath, Path
from google.cloud import bigquery
from datetime import datetime, timedelta
import codecs
import tarfile

bqclient = bigquery.Client()

try:
    from yaml import CLoader as Loader
except ImportError:
    from yaml import Loader

parser = argparse.ArgumentParser(description='Generator parser')
parser.add_argument('--path', dest='filepath', help='Generator config file')
parser.add_argument('--workdir', dest='workdir', help='Working directory')

args = parser.parse_args()

config_path = PurePath(args.workdir, args.filepath)

config = load(open(config_path).read(), Loader=Loader)

SOURCE = config['source']
DATASET = config['dataset']
TABLES = config['tables']
EXTRACT = config['extract_type']
FORMAT = config['format']
RAW = PurePath(args.workdir, "data")
DATETYPE = config['date_type']
TIMESTAMP = datetime.now().isoformat()
load_date = '"{0}" as load_date'.format(TIMESTAMP)

try:
    DATE = datetime.strptime(config['extract_date'], '%Y-%m-%d').date().isoformat()
except:
    yesterday = datetime.today() - timedelta(days=1)
    DATE = yesterday.date().isoformat()

def build_query(table):
    query = '''
        select
        {0},
        {3}
        from
        {1}.{2}
    '''.format(table['columns'], DATASET, table['name'], load_date)

    if EXTRACT != 'full':
        if DATETYPE == 'equal':
            operator = '='
        elif DATETYPE == 'less_than':
            operator = '<'
        elif DATETYPE == 'bigger_equal':
            operator = '>='

        query = query + ' where {0} {2} "{1}" '.format(table['date_column'], DATE, operator)

    if config['environment'] == 'dev':
        query = query + ' limit 100 '
    
    return query

def load_data(query):
    dataframe = (
    bqclient.query(query)
    .result()
    .to_dataframe(
        # Optionally, explicitly request to use the BigQuery Storage API. As of
        # google-cloud-bigquery version 1.26.0 and above, the BigQuery Storage
        # API is used by default.
        create_bqstorage_client=True,
        )
    )
    return dataframe

def to_output(data, final_path):
    if FORMAT == 'txt':
        output = data.to_csv(sep=config['separator'], header=True, quotechar='"', line_terminator='\n', index=False)
        with codecs.open('{0}.txt'.format(final_path), "w", "utf-8-sig") as writefile:
            writefile.writelines(output)
    
    elif FORMAT == 'parquet':
        return data.to_parquet(path='{0}.parquet'.format(final_path), engine='auto', compression='snappy', index=None)
    
    else:
        return 1

def write_output(data, table, rundate, summary, folder):
    output_file = '{source}_{table}_{rundate}'.format(
        source = SOURCE.upper(), 
        table = table['name'].upper(), 
        rundate = rundate)
    final_path = folder.joinpath(output_file)
    rows = len(data.index)
    to_output(data, final_path)
    
    summary_path = folder.joinpath(summary)
    with open(summary_path, 'a') as summmaryfile:
        summmaryfile.write('{filename}|{count}\n'.format(filename=output_file, count=rows))
    
    return (final_path, summary_path)

if __name__ == '__main__':
    rundate = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_folder = '{source}_{rundate}'.format(source=SOURCE, rundate=rundate)
    folder = RAW.joinpath(output_folder)
    os.mkdir(folder)
    summary = 'SUMMARY_{source}_{rundate}.txt'.format(source=SOURCE.upper(), rundate=rundate)
    os.chdir(RAW)
    tar = tarfile.open('{filename}.tar.gz'.format(filename=output_folder), "w:gz")
    final_summary = ''

    for table in TABLES:
        query = build_query(table)
        data = load_data(query)
        final_output, final_summary = write_output(data, table, rundate, summary, folder)
    tar.add(output_folder)
