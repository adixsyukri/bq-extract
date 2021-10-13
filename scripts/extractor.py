import argparse
import os
import sys
from pathlib import PurePath, Path
from datetime import datetime, timedelta
import codecs
import tarfile
from engine import LocalEngine, DataflowEngine, get_engine
from table import TableModel
from log import set_log, sys_exit

try:
    from yaml import load, CLoader as Loader
except ImportError:
    from yaml import load, Loader

parser = argparse.ArgumentParser(description='Generator parser')
parser.add_argument('--path', dest='filepath', help='Generator config file')
parser.add_argument('--table', dest='table_name', help='job for table')
parser.add_argument('--rundate', dest='rundate', help='job rundate YYYY-MM-DDTHH:mm:ss.S')

args, pipeline_args = parser.parse_known_args()

filepath = args.filepath
table_name = args.table_name
job_datetime = args.rundate
config = load(open(PurePath(filepath)).read(), Loader=Loader)

logname = 'bq-extract-{}'.format(table_name)
logger = set_log(config, logname)

def output_settings():
    datadir = config['output']['datadir']
    RAW = PurePath(datadir,'raw')
    timestamp = datetime.fromisoformat(job_datetime)
    load_date = '"{0}" as load_date'.format(timestamp)
    rundate = timestamp.strftime('%Y%m%d_%H%M%S')
    output_folder = '{source}_{rundate}'.format(source=config['source']['name'], rundate=rundate)
    folder = RAW.joinpath(output_folder)
    
    try:
        os.mkdir(folder)
    except Exception:
        logger.info("Directory exists: {}".format(folder))

    summary = 'SUMMARY_{source}_{rundate}.txt'.format(source=config['source']['name'].upper(), rundate=rundate)

    try:
        DATE = config['source']['extract_date'].isoformat()
        logger.info("Extract exact records date: {}".format(DATE))
    except:
        yesterday = datetime.today() - timedelta(days=1)
        DATE = yesterday.date().isoformat()
        logger.info("Extract T-1 records: {}".format(DATE))
    
    settings = {
        'rundate': rundate,
        'date': DATE,
        'load_date': load_date,
        'output_folder': output_folder,
        'folder': folder,
        'summary': summary,
        'timestamp': timestamp,
        'datadir': datadir,
        'raw': RAW,
    }

    return settings

def run_main():
    logger.info("Extraction Begin: {}".format(table_name))
    TABLES = config['tables']

    settings = output_settings()
    engine = get_engine(config, settings)

    # Extract data from tables recursively
    for tableinfo in TABLES:
        if table_name != tableinfo['name']:
            continue
        table = TableModel(config, tableinfo, engine, settings)
        logger.info("compose query for: {}".format(table.info['name']))
        query = table.compose_query()
        logger.info("Getting data for: {}".format(table.info['name']))
        data = table.get_data(query)
        logger.info("Writing data for: {}".format(table.info['name']))
        table.write_output(data) 
    logger.info("Extraction Done: {}".format(table_name))

if __name__ == '__main__':
    run_main()
