from multiprocessing import (
    Process,
    Event,
    Queue,
    Pool
)
from threading import Thread
import os, sys
import ssl
import math
import json
import paho
import time
import paho.mqtt.client as mqtt
from paho.mqtt.packettypes import PacketTypes
import paho.mqtt.publish as publish
from collections import defaultdict
from paho.mqtt.client import (
    MQTTMessage, 
    error_string, 
    connack_string
)
import logging
import time
from common.configutils import get_hparams_from_commentjson
from common.common import (
    FileShareInfo,
    UserDataKeys,
    TransferMode,
    get_random_string
)

'''
function: init mqtt client from configuration file 
return: mqtt client: 
NOTE: the callback function should be added separately after setup client
'''
def init_mqtt5(config_file): 
    cfg = get_hparams_from_commentjson(config_file)
    cfgserver = cfg.server
    cfgTopic = cfg.topic
    #tls = cfgserver.tls
    topic_mcu = cfgTopic.topic_mcu
    topic_broker = cfgTopic.topic_broker
    # 1. check certification file if tls mode
    if cfgserver.tls: 
        f_ca = cfgserver.server_ca
        f_cc = cfgserver.cli_cert
        f_ck = cfgserver.cli_key
        if not os.path.isfile(f_ca): 
            logging.error(f'Cannot find server certification file: {cfgserver.server_ca}')
            return None
        if cfgserver.cli_verify: # check client certificate files
            if not os.path.isfile(f_cc) \
                or not os.path.isfile(f_ck): 
                logging.error(f"Cannot find client certification files: {f_cc}:{f_ck}")
                return None
    # 2. check if topic is empty
    if not topic_mcu or not topic_broker: 
        logging.error('Topics of file request and response can\'t be empty' )
        return None
    # 3. check qos
    if int(cfgserver.qos ) not in [0,1,2]: 
        logging.info(f'invalid QoS, it should be in [0,1,2] range')
        return False
    # init mqtt
    mqttc = mqtt.Client(client_id="", protocol=mqtt.MQTTv5)

    if cfgserver.tls:
        logging.debug('configure client with TLS')
        tlsVersion = ssl.PROTOCOL_TLSv1_2

        if not cfgserver.insecure:
            cert_required = ssl.CERT_REQUIRED
        else:
            cert_required = ssl.CERT_NONE
        
        if cfgserver.cli_verify: # client verify
            mqttc.tls_set(
                ca_certs=cfgserver.server_ca, 
                certfile=cfgserver.cli_cert, 
                keyfile=cfgserver.cli_key, 
                cert_reqs=cert_required, 
                tls_version=tlsVersion)
        else: # no need client verify. only server verify
            mqttc.tls_set(
                ca_certs=cfgserver.server_ca, 
                cert_reqs=cert_required, 
                tls_version=tlsVersion)
        if cfgserver.insecure:
            mqttc.tls_insecure_set(True)
    return mqttc





class FileSenderProcessor(Thread): 
    def __init__(self, config_file: str, 
                 stopflag: Event, transMode: int): 
        super().__init__()        
        self.init = False
        # check configuration file first
        self.cfg_file = config_file
        if not os.path.isfile(config_file): 
            logging.error(f'Cannot find configuration file: {config_file}')
            return
        # init mqtt client library
        self.transmode = TransferMode.MQTT
        if transMode == TransferMode.HTTP: 
            self.transmode = TransferMode.HTTP
            
        if not self.init_mqtt5(config_file): 
            logging.error('failed to init mqtt client')
            return
        self.meta = 'Sender: '
        self.sessions = defaultdict(lambda:FileShareInfo)   # dictionary for file to be processed now()
        self.stop = stopflag
        self.chunsize = 512 # chunck size in byte
        self.init = True
    
    # load configuration
    def init_mqtt5(self, config_file)->bool: 
        self.cfg = get_hparams_from_commentjson(config_file)
        self.cfgserver = self.cfg.server
        self.cfgTopic = self.cfg.topic
        self.topic_mcu = self.cfgTopic.topic_mcu            # mcu topic channel
        self.topic_broker = self.cfgTopic.topic_broker     # broker topic channel
        self.valid_topics = [self.remove_wildcard(self.topic_mcu), self.remove_wildcard(self.topic_broker)]
        self.qos = self.cfgserver.qos
        self.mqttc = init_mqtt5(config_file)
        if not self.mqttc: 
            logging.fatal('failed to configure mqtt client')
            return False
            
        # set callback
        if self.transmode == TransferMode.MQTT: 
            self.mqttc.on_message = self.on_message
        else: # http mode
            self.mqttc.on_message = self.on_message2
        self.mqttc.on_connect = self.on_connect
        self.mqttc.on_connect_fail = self.on_connect_fail
        self.mqttc.on_disconnect = self.on_disconnect
        self.mqttc.on_publish = self.on_publish
        self.mqttc.on_subscribe = self.on_subscribe
        self.mqttc.on_unsubscribe = self.on_unsubscribe
        self.mqttc.on_log = self.on_log
        logging.info(f'Connecting to {self.cfgserver.host}:({self.cfgserver.port})...')
        return True
    '''
    return the prefix of topic after removing the wildcard
    '''
    def remove_wildcard(self, topic:str)->str: 
        if topic.endswith('/+'): 
            topic = topic[:-2]
        elif topic.endswith('/#'): 
            topic = topic[:-2]
        return topic

    '''
    check if the input topic is valid
    '''
    def is_valid_topic(self, topic_input: str)->bool: 
        for topic in self.valid_topics: 
            if topic_input.find(topic) == 0: 
                    return True
        return False
                
                



    # The MQTTv5 callback takes the additional 'props' parameter.
    def on_connect(self,mqttc, userdata, flags, rc, props = None):
        #global client_id, reply_to
        logging.debug(f"{self.meta} Connected: {flags}, error code = {rc}, property = {props}")
        
    def on_connect_fail(self, mqttc, userdata): 
        logging.error("{self.meta}Failed to connect")

    def on_disconnect(self, client, userdata, reasonCode, properties): 
        logging.info(f"{self.meta}Disconnected: reasoncode = {reasonCode}")


    def on_message2(self, mqttc, userdata, msg: MQTTMessage): 
        global reply
        logging.info(f'{self.meta}On Message: topic = {msg.topic}, payload = {msg.payload}, property = {msg.properties}')
        
        # check correlation data
        topic = msg.topic
        
        props = dict()
        if hasattr(msg.properties, 'UserProperty'): # load user property
            props = dict(msg.properties.UserProperty)
        logging.debug(f"receive file sharing request : user property: {topic}: {props}")

        
        if self.is_valid_topic(topic): # first request of file sharing
            # check response topic
            if not hasattr(msg.properties, 'ResponseTopic'): # esp32 want to send file into this topic
                logging.error("message doesn't have reponse topic. Ignore this message")
                return
            topic_send = msg.properties.ResponseTopic
            logging.debug(f"{self.meta} response topic: {topic_send}")

            # generate correlation id
            transID = ''
            transID = get_random_string(8)
            
            # create reponse topic of send topic
            topic_send_rep = f'{topic_send}/{transID}'
            self.mqttc.subscribe(topic_send_rep, qos = self.qos)
            self.publish_download_url( topic_send, topic_send_rep, transID, props)
            logging.info(f'{self.meta}: subscribt into file transfer response topic: {topic_send_rep}')
            return
    

    # An incoming message should be the reply to our request
    def on_message(self, mqttc, userdata, msg: MQTTMessage):
        global reply
        logging.info(f'{self.meta}On Message: topic = {msg.topic}, payload = {msg.payload}, property = {msg.properties}')
        
        # check correlation data
        topic = msg.topic
        
        props = dict()
        if hasattr(msg.properties, 'UserProperty'): # load user property
            props = dict(msg.properties.UserProperty)
        logging.debug(f"receive file sharing request : user property: {topic}: {props}")

        
        if self.is_valid_topic(topic): # first request of file sharing
            # check response topic
            if not hasattr(msg.properties, 'ResponseTopic'): # esp32 want to send file into this topic
                logging.error("message doesn't have reponse topic. Ignore this message")
                return
            topic_send = msg.properties.ResponseTopic
            logging.debug(f"{self.meta} response topic: {topic_send}")

            # generate correlation id
            transID = ''
            while True: 
                transID = get_random_string(8)
                if transID not in self.sessions.keys(): 
                    break

            # create reponse topic of send topic
            topic_send_rep = f'{topic_send}/{transID}'
            self.mqttc.subscribe(topic_send_rep, qos = self.qos)
            self.publish_file( topic_send, topic_send_rep, transID, props)
            logging.info(f'{self.meta}: subscribt into file transfer response topic: {topic_send_rep}')
            return

        if hasattr(msg.properties, "CorrelationData"): # request for next chunk
            transID = msg.properties.CorrelationData.decode('utf-8')
            if transID not in self.sessions.keys(): # reply for previous file transfer
                logging.error("Invalid session key: {}".format(transID))
                return
            logging.debug(f"correlation data(session key) = {transID}")
            
            # parse response
            res = self.str2bool(props[UserDataKeys.RESULT])
            completed = self.str2bool(props[UserDataKeys.COMPLETED])
            if completed:  # 
                topic_response = self.sessions[transID].topic_reply
                self.mqttc.unsubscribe(topic_response)
                logging.info(f'success to send all file: {self.sessions[transID].filePath}')
                del self.sessions[transID]
                return

            if res: # increase chunk index
                self.sessions[transID].chk_idx  = int(props[UserDataKeys.CHUNK_ID]) + 1
                logging.info(f'success to receive data, increase chunk index: chunk_id = {self.sessions[transID].chk_idx}')
            else: 
                self.sessions[transID].chk_idx  = int(props[UserDataKeys.CHUNK_ID])
                logging.warn(f'faile to receive data, send again = {self.sessions[transID].chk_idx}')
            
            self.send_one_chunk(self.sessions[transID])
            return
            
    def on_publish(self, mqttc, obj, mid):
        logging.info(f"{self.meta}On publish: message ID = {mid}")

    def on_subscribe(self, mqttc, obj, mid, reasoncodes, properties = None):
        # print("Subscribed: " + str(mid) +  " " + str(properties))
        logging.info(f"{self.meta}:Subscribed: message id: {mid}, properties: {properties}")
        # for code in reasoncodes: 
        #     print(f'reason codes: {code.value}: {code.getName()}')


    def on_unsubscribe(self, client, userdata, mid, properties, reasonCodes): 
        logging.info(f"{self.meta}Unsubscribed: reason code: {reasonCodes}, message ID = {mid}")

    def on_log(self, mqttc, obj, level, string):
        logging.info(f'{self.meta} Log message: {level}: {string}')    

    # stop client
    def stop_client(self): 
        self.mqttc.disconnect()
        self.mqttc.loop_stop()

    """
    Summary: get file name from property
        It should determine the correct file path by analyzing the payload and property json string. 
        It should be implemented for how to analyze the file sharing request
    """
    def get_file_from_property(self, properties): 
        # return the default file name now
        return self.cfg.file.src

    """
    Summary: get file name from property
        It should determine the correct file url by analyzing the payload and property json string. 
        It should be implemented for how to analyze the file sharing request
    """
    def get_download_url_from_property(self, properties): 
        # if new version isn't available, return empty string
        return self.cfg.file.srcurl


    def publish_download_url(self,  topic_send, topic_reply, transactionID, props): 
        ...        
        """
        topic_send: topic to send a file
        topic_reply: toic to receive a reponse
        transactionID: transactio id of file transfer
        payload: dictionary data of request
        props: property of request
        """
        logging.debug(f"{self.meta}receive file transfer request: \n\t topic = {topic_send}, \n\tresponseTopic = {topic_reply}, \n\tproperties: {props}")
        
        # check file name from payload and properties in real product
        url = self.get_download_url_from_property(props)
        props = mqtt.Properties(PacketTypes.PUBLISH)
        props.ResponseTopic = topic_reply
        props.CorrelationData = bytearray(transactionID.encode('utf-8'))
        props.PayloadFormatIndicator = 0

        props.UserProperty = (UserDataKeys.FIRM_URL, url)
        _ = self.mqttc.publish(topic= topic_send, payload=b' ', qos = self.qos, retain=False, properties=props)

    
        
    # send a first packet of one file 
    def publish_file(self, topic_send, topic_reply, transactionID, props): 
        """
        topic_send: topic to send a file
        topic_reply: toic to receive a reponse
        transactionID: transactio id of file transfer
        payload: dictionary data of request
        props: property of request
        """
        logging.debug(f"{self.meta}receive file transfer request: \n\t topic = {topic_send}, \n\tresponseTopic = {topic_reply}, \n\tproperties: {props}")
        
        # check file name from payload and properties in real product
        file = self.get_file_from_property(props)
        if not os.path.isfile(file): 
            logging.error('Canot find file: {}'.format(file))
            return
        filesize = os.stat(file).st_size # file size
        nbrChunks = math.ceil(filesize / self.chunsize)
        self.sessions[transactionID] = FileShareInfo(
            topic_send, topic_reply, transactionID, transactionID,
            file, filesize, self.chunsize, nbrChunks, 0)
        # send first chunk for new request
        self.send_one_chunk(self.sessions[transactionID])
        

    def send_one_chunk(self, fileInfo: FileShareInfo): 
        chunk_index = fileInfo.chk_idx
        if chunk_index >= fileInfo.total_chks: 
            logging.info(f"{self.meta}: send all chunks of file{fileInfo.filePath}: {chunk_index}")
            self.completed = True
            return
        
        logging.info(f'{self.meta}publising of {chunk_index}/{fileInfo.total_chks} chunk')
        with open(fileInfo.filePath, "rb") as f: 
            chunk_position = self.chunsize* chunk_index
            f.seek(chunk_position)
            content = f.read(self.chunsize)
            data = bytearray(content)
            logging.debug(f"chunk size: {len(data)}, type = {type(data)}")
            # publish now
            props = mqtt.Properties(PacketTypes.PUBLISH)
            props.CorrelationData = fileInfo.corr_data
            props.ResponseTopic = fileInfo.topic_reply
            props.PayloadFormatIndicator = 0

            props.UserProperty = (UserDataKeys.FILE_ID, str(fileInfo.fileid))
            props.UserProperty = (UserDataKeys.FILE_NAME, os.path.basename(fileInfo.filePath))
            props.UserProperty = (UserDataKeys.FILE_SIZE, str(fileInfo.filesize))
            props.UserProperty = (UserDataKeys.POSITION, str(chunk_position))
            props.UserProperty = (UserDataKeys.CHUNK_SIZE, str(len(content)))
            props.UserProperty = (UserDataKeys.CHUNK_ID, str(chunk_index))
            
            _ = self.mqttc.publish(topic= fileInfo.topic_send, payload=data, qos = self.qos, retain=False, properties=props)
            #msgInfo.wait_for_publish()
            
        logging.info(f'{self.meta}success to publish {chunk_index}/{fileInfo.total_chks}:{fileInfo.corr_data}, response topic ={fileInfo.topic_reply}')

    def str2bool(self, str): 
        return str.lower() in ['1', 'true', 'yes', 'ok']


    def run(self): 
        if not self.init: 
            logging.error(f'{self.meta}failed to init mqtt client')
            return
        logging.info('starting mqtt file sender process')
        self.mqttc.connect(self.cfgserver.host, self.cfgserver.port, int(self.cfgserver.keep_alive))
        # subscribe into MCU firmware channel
        self.mqttc.subscribe(self.topic_mcu, self.cfgserver.qos)
        # subscribe into Broker firmware channel
        self.mqttc.subscribe(self.topic_broker, self.cfgserver.qos)

        self.mqttc.loop_forever(retry_first_connection = True)
        #self.mqttc.loop_start()
        logging.info(f'{self.meta}exiting file sender process')

