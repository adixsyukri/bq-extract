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
parser.add_argument('--rundate', dest='rundate', help='job rundate YYYY-MM-DDTHH:mm:ss.S')

args, pipeline_args = parser.parse_known_args()

filepath = args.filepath
job_datetime = args.rundate
config = load(open(PurePath(filepath)).read(), Loader=Loader)

logger = set_log(config)

def output_settings():
    datadir = config['output']['datadir']
    raw = PurePath(datadir,'raw')
    outbound = PurePath(datadir,'outbound')
    timestamp = datetime.fromisoformat(job_datetime)
    rundate = timestamp.strftime('%Y%m%d_%H%M%S')
    output_folder = '{source}_{rundate}'.format(source=config['source']['name'], rundate=rundate)
 
    settings = {
        'output_folder': output_folder,
        'raw': raw,
        'outbound': outbound
    }

    return settings

def run_main():
    settings = output_settings()
    logger.info("Compression Begin: {}".format(settings['output_folder']))
    # Change directory to data folder
    compressed_file = settings['outbound'].joinpath(settings['output_folder'])
    tar = tarfile.open('{filename}.tar.gz'.format(filename=compressed_file), "w:gz")

    try:
        os.chdir(settings['raw'])
    except Exception:
        logger.error("Fail changing directory", exc_info=True)
        sys_exit()

    # Archive and compress folder
    try:
        tar.add(settings['output_folder'])
    except Exception:
        logger.error("Fail archiving output folder", exc_info=True)
        sys_exit()
    logger.info("Compression Done: {}".format(settings['output_folder']))

if __name__ == '__main__':
    run_main()