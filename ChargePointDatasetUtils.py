# ChargePointDatasetUtils.py

import logging, os

def get_logger(name, cwd, log_path):
  # initialize the logger
  logger = logging.getLogger(name)
  logger.setLevel(logging.INFO)

  fh = logging.FileHandler(os.path.join(cwd, log_path))
  fh.setLevel(logging.INFO)

  formatstr = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
  formatter = logging.Formatter(fmt=formatstr, datefmt='%Y-%m-%d %H:%M:%S')

  fh.setFormatter(formatter)
  logger.addHandler(fh)

  return logger
