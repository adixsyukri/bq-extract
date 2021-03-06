import logging
from pathlib import PurePath, Path
import os
import sys
from datetime import datetime, timedelta

def sys_exit():
    sys.exit("script ended with failure")

def set_log(config, logname):
    
    logdate = datetime.now().strftime('%Y%m%d')
    logdir = PurePath(config['log'], logdate)
    logger = logging.getLogger('Bigquery Extraction')
    
    # Check bq extract log folder exists or not, manually create the folder if not exists
    try:
        os.path.isdir(config['log'])
    except Exception:
        logger.error("Check logs directory error", exc_info=True)
        sys_exit()
    
    # Create folder Log with date
    try:
        os.mkdir(logdir)
    except Exception:
        logger.info("Log date folder exists")
    
    logging.basicConfig(
        level=logging.INFO, 
        filename=PurePath(logdir,'{}.log'.format(logname)),
        format='%(asctime)s :: %(levelname)s :: %(message)s')
    
    return logger
