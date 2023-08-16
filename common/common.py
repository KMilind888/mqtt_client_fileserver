import os
import random
import string
import logging


# initialize global logger

def init_logger(logName = 'App.log', terminal = True):
    FORMAT = "[%(levelname)s %(filename)s:%(lineno)s - %(funcName)s() %(asctime)s ]: %(message)s"
    if terminal:
        logging.basicConfig(level=logging.DEBUG, filemode='w', format=FORMAT, datefmt='%d-%b-%y %H:%M:%S')
    else:
        logging.basicConfig(filename='Log/{}'.format(logName), level=logging.DEBUG, filemode='w', format=FORMAT, datefmt='%d-%b-%y %H:%M:%S')
    logging.debug('System successfully launched')


class UserDataKeys:
    FILE_ID = 'FID'                 # file transaction ID
    FILE_NAME = 'FILE_NAME'         # file name 
    FILE_SIZE = "SIZE"              # total file size in bytes
    POSITION = 'POSITION'
    CHUNK_SIZE = "DATA_LENGTH"       # one chunk size in bytes
    CHUNK_ID =  "CHUNK_IDX"         # chunk index of current packet
    COMPLETED = "COMPLETED"         # entire download is completed? 
    RESULT  = "PCK_RES"             # result of one packet transfer
    


def get_random_string(length):
    # choose from all lowercase letter
    letters = string.ascii_lowercase
    result_str = ''.join(random.choice(letters) for i in range(length))
    print("Random string of length", length, "is:", result_str)
    return result_str


class FileShareInfo: 
    def __init__(self, 
                 topic_send: str,       # topic of send channel
                 topic_send_rep: str,   # topic of response channel
                 corr_data: str,        # correlation data
                 fileid: str,
                 file: str,             # file path in server
                 file_size: int,        # total file size in byte
                 chk_size = 1024,       # on chunk size in byte
                 total_chks = 1,        # the total count of chunks in this file
                 idx = 0):              # chunk index to be sent
        self.topic_send = topic_send
        self.topic_reply = topic_send_rep
        self.corr_data = bytearray(corr_data.encode('utf-8'))
        self.fileid =fileid
        self.filePath = file
        self.filesize = file_size
        self.chk_size = chk_size
        self.total_chks = total_chks
        self.chk_idx = idx