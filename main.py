from multiprocessing import Queue, Event
from common.common import init_logger
import logging
from fileSender import FileSenderProcessor
from fileReceiver import FileReceiverProcessor


if __name__ == '__main__': 
    init_logger('mqtt.log')
    logging.info('start processing')
    stop_flag = Event()
    cfg_file = 'common/config.json'
    sender = FileSenderProcessor(config_file = cfg_file, stopflag=stop_flag)
    #receiver = FileReceiverProcessor(config_file = cfg_file, stopflag=stop_flag)
    #receiver.start()
    sender.run()
    #receiver.join()
    #sender.stop_client()
    logging.info('exiting process')
    