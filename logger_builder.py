import logging
import datetime

logger = logging.getLogger('cryptotrading logger')
logger.setLevel(logging.DEBUG)
file_path = 'h:\\traderBot_log\\' + datetime.date.today().strftime('%Y-%m-%d.log')
fh = logging.FileHandler(file_path)
fh.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
fh.setFormatter(formatter)
ch.setFormatter(formatter)
logger.addHandler(fh)
logger.addHandler(ch)

logger.info('logger initiated')