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
import paho.mqtt.client as mqtt
from paho.mqtt.packettypes import PacketTypes

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
    tls = cfgserver.tls
    topic_resp = cfgTopic.topic_response
    topic_req = cfgTopic.topic_request
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
    if not topic_req or not topic_resp: 
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


class FileWriter:
    def __init__(self, file = 'test.dmp'):
        self.file = file
        self.total_chunk = 0
        self.chk_idx = -1
        self.chk_size = 0
        self.file_size = 0
        self.written = 0
        if os.path.isfile(self.file): 
            os.remove(self.file)
        self.inited = False
        
    def init_param(self, file_size:int, total_chk: int, chk_size: int): 
        self.file_size = file_size
        self.total_chunk = total_chk
        self.chk_size = chk_size
        self.inited = True
    
    def add_data(self, file_size, data, chk_idx)->bool: 
        if not self.inited: 
            logging.warn("File Writter is not initialized")
            return False
        if chk_idx < self.chk_idx: # duplicated chun
            logging.warn("duplicated chunk")
            return False
        if file_size != self.file_size: 
            logging.error("unmatched file size. return")
        temp = self.written + len(data)
        if temp > self.file_size: 
            logging.warn("invalid packet data which exceeds the target file size")
            return False
        # write binary data into file
        self.chk_idx = chk_idx
        mode = 'wb' if self.written <= 0 else 'ab'
        with open(self.file, mode) as f: 
            f.write(data)
        self.written += len(data)
        return True
    
    def last_chk_id(self): 
        return self.chk_idx
    
    def is_completed(self): 
        if self.written == self.file_size: 
            return True
        elif self.written < self.file_size: 
            print(f'insufficient data: {self.written}/{self.file_size}')
        else: 
            print(f'too much data: {self.written}/{self.file_size}')
        return False
    def is_inited(self): 
        return self.inited
    
    def __str__(self):
        return f'written chunk: {self.chk_idx}/{self.total_chunk}, written byttes: {self.written}/{self.file_size}'


class FileReceiverProcessor(Thread): 
    def __init__(self, config_file: str, 
                 stopflag: Event): 
        super().__init__()        
        self.init = False
        # check configuration file first
        self.cfg_file = config_file
        if not os.path.isfile(config_file): 
            logging.error(f'Cannot find configuration file: {config_file}')
            return
        # init mqtt client library
        if not self.init_mqtt5(config_file): 
            logging.error('failed to init mqtt client')
            return
        
        self.meta = "Receiver:"
        self.writer = FileWriter(file = self.cfg.file.dst)
        self.completed = False
        self.stop = stopflag
        self.init = True

    # load configuration
    def init_mqtt5(self, config_file)->bool: 
        self.cfg = get_hparams_from_commentjson(config_file)
        self.cfgserver = self.cfg.server
        self.cfgTopic = self.cfg.topic
        self.topic_req = self.cfgTopic.topic_request        # topic of request
        self.topic_resp = self.cfgTopic.topic_response      # topic of reply, could be modified later
        self.qos = self.cfgserver.qos
        self.mqttc = init_mqtt5(config_file)
        self.clientID = ''
        if not self.mqttc: 
            logging.fatal('failed to configure mqtt client')
            return False
            
        # set callback
        self.mqttc.on_message = self.on_message
        self.mqttc.on_connect = self.on_connect
        self.mqttc.on_connect_fail = self.on_connect_fail
        self.mqttc.on_disconnect = self.on_disconnect
        self.mqttc.on_publish = self.on_publish
        self.mqttc.on_subscribe = self.on_subscribe
        self.mqttc.on_unsubscribe = self.on_unsubscribe
        self.mqttc.on_log = self.on_log
        logging.info(f'Connecting to {self.cfgserver.host}:({self.cfgserver.port})...')
            
        return True

    # The MQTTv5 callback takes the additional 'props' parameter.
    def on_connect(self,mqttc, userdata, flags, rc, props = None):
        logging.debug(f"{self.meta}:Connected: {flags}, error code = {rc}, property = {props}")
        # if int(rc) >= 128: # 0x80
        #     logging.error(f'failed to connect, error code = {rc}')
        
        if hasattr(props, 'AssignedClientIdentifier'):
            self.clientID = str(props.AssignedClientIdentifier)
            
        self.topic_resp = self.topic_resp + '/' + self.clientID
        logging.info(f'{self.meta}:subscribe to response topic: {self.topic_resp}')
        mqttc.subscribe(self.topic_resp) # subscribe to topic of response is expected channel
        self.pubish_sharing_request()
        self.connected = True
        
    def on_connect_fail(self, mqttc, userdata): 
        logging.error(f"{self.meta}:Failed to connect")

    def on_disconnect(self, client, userdata, reasonCode, properties): 
        logging.info(f"{self.meta}:Disconnected: reasoncode = {reasonCode}")
        self.connected = False


    # An incoming message should be the reply to our request
    def on_message(self, mqttc, userdata, msg: MQTTMessage): # waiting for file sharing
        topic = msg.topic
        #payload = msg.payload.decode('utf-8') # decode bytes into string
        payload = msg.payload
        logging.info(f'{self.meta}: receive message = {topic}, payload = {len(payload)}')
        
        if(msg.topic == self.topic_resp): 
            # read property first
            props = msg.properties
            if not hasattr(props, "UserProperty"): 
                logging.error(f"{self.meta}:no valid user property")
                return
            if not hasattr(props, "ResponseTopic"): 
                logging.error(f"{self.meta}:no valid reponse topic")
                return
            if not hasattr(props, "CorrelationData"): 
                logging.error(f"{self.meta}:no valid CorrelationData")
                return
            
            userprop = dict(props.UserProperty)
            topic_ack = props.ResponseTopic
            corrData = props.CorrelationData
            logging.debug(f'{self.meta}: received file data. property = {userprop}')
            # check file property
            try:
                file_size = int(userprop[UserDataKeys.FILE_SIZE])
                chunk_size = int(userprop[UserDataKeys.CHUNK_SIZE])
                chunk_id = int(userprop[UserDataKeys.CHUNK_ID])
                chunk_total = int(userprop[UserDataKeys.TOTAL_CHUNK])
            except ValueError: 
                logging.error(" Insufficient user data keys. failed to extract file meta data.")
                return

            # init file writer
            if not self.writer.is_inited():
                self.writer.init_param(file_size, chunk_total, chunk_size)
            # write data into file
            res_write = self.writer.add_data(file_size, payload, chunk_id)
            self.completed = self.writer.is_completed()

            userprop[UserDataKeys.RESULT] = res_write
            userprop[UserDataKeys.COMPLETED] = self.completed
            print(f'{self.meta}:file writter state: {self.writer}')
            self.send_ack_signal(topic_ack, corrData, userprop)

        else: 
            logging.info(f"{self.meta}receive message in invalid topic")
        
    def on_publish(self, mqttc, obj, mid):
        logging.info(f"On publish: message ID = {mid}")


    def on_subscribe(self, mqttc, obj, mid, reasoncodes, properties = None):
        # print("Subscribed: " + str(mid) +  " " + str(properties))
        logging.info(f"{self.meta}: Subscribed: message id: {mid}, properties: {properties}")
        # for code in reasoncodes: 
        #     print(f'reason codes: {code.value}: {code.getName()}')


    def on_unsubscribe(self, client, userdata, mid, properties, reasonCodes): 
        logging.info(f"{self.meta}Unsubscribed: reason code: {reasonCodes}, message ID = {mid}")

    def on_log(self, mqttc, obj, level, string):
        logging.info(f'{self.meta}:{self.meta}:Log message: {level}: {string}')    

    # stop client
    def stop_client(self): 
        self.mqttc.disconnect()
        self.mqttc.loop_stop()


    # send the file sharing request
    def pubish_sharing_request(self): 
        logging.info('publishing file sharing request')
        pyl_data = dict()
        pyl_data["firmware_name"] = "SIP Inductive"
        pyld = json.dumps(pyl_data)
        props = mqtt.Properties(PacketTypes.PUBLISH)
        props.CorrelationData = b'file sharing request' # set can any password data here
        props.ResponseTopic = self.topic_resp
        props.UserProperty = ("vendor_version", "v1.2.0")
        props.UserProperty = ("update_date", "2023-07-01")
        self.mqttc.publish(topic = self.topic_req, payload = pyld, qos = self.qos, retain=False,properties= props)
    

    def send_ack_signal(self, topic_ack, corrData, pyld:dict): 
        # prepare payload json data
        payload = json.dumps(pyld)
        props = mqtt.Properties(PacketTypes.PUBLISH)
        if type(corrData) == type(''): 
            props.CorrelationData = corrData.encode('utf-8') # set can any password data here
        elif type(corrData) == type(b''):
            props.CorrelationData = corrData # set can any password data here
        self.mqttc.publish(topic = topic_ack, payload = payload, qos = self.qos, retain=False, properties= props)
        logging.info(f"send file-trans ack data into {topic_ack}, payload = {payload}")
        
    
    def run(self): 
        if not self.init: 
            logging.error('failed to init mqtt client')
            return
        logging.info('starting mqtt file receiver process')
        self.mqttc.connect(self.cfgserver.host, self.cfgserver.port, int(self.cfgserver.keep_alive))

        #self.mqttc.loop_forever(retry_first_connection = True)
        self.mqttc.loop_start()
        logging.info('Receiver success to connect ')
        # send file sharing request
        self.pubish_sharing_request()


        while True:
            if self.completed: 
                break
            if self.stop.is_set(): 
                break
            time.sleep(0.1)
        self.mqttc.disconnect()
        
        logging.info('exiting file sender process')