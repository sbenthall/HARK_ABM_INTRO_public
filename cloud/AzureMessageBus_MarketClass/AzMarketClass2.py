#!/usr/bin/env python3
#from sharkfin.utilities import *
#from sharkfin.markets import AbstractMarket
import numpy as np
import json
import uuid
import time
import os
import uuid
import threading
import session_info
from azure.servicebus import ServiceBusClient, ServiceBusMessage
from azure.servicebus.management import ServiceBusAdministrationClient
from azure.keyvault.secrets import SecretClient
from azure.identity import DefaultAzureCredential

class ClientRPCMarket():
    def __init__(self, seed_limit='', session_id='', keyVault=''):
        self.simulation_price_scale = 1
        self.default_sim_price = 100
        seed_limit = None
        self.sample = 0
        self.seeds = []
        self.seed_limit = seed_limit
        self.latest_price = None
        self.prices = [self.default_sim_price]
        self.rpc_session_id = session_id #new property
        self.rpc_send_queue_name = session_id +'_requests' #new property
        self.rpc_response_queue_name = session_id +'_responses' #new property
        self.rpc_messageBus_namespace = 'sharfinmq' #new property
        self.connection_str = self.get_rpc_connectionString(keyVault) #new property
        self.init_az_rpc() 
        self.create_queues() #new method
        #setup lists for sent and received messages.
        self.sentMsgHistory = []      #new property
        self.recvMsgHistory = []      #new property
        
    #method to initialize Azure Service Bus Clients for messageing and managment
    def init_az_rpc(self):
        self.service_bus_mgmt_client = ServiceBusAdministrationClient.from_connection_string(self.connection_str)
        self.service_bus_message_client = ServiceBusClient.from_connection_string(conn_str=self.connection_str, logging_enable=True)
        
        
    #method to create and delete the required sending and response queues for RPC pattern
    def create_queues(self):
        #create queue for sharkfin to send daily values
        self.service_bus_mgmt_client.create_queue(self.rpc_send_queue_name, max_delivery_count=10, dead_lettering_on_message_expiration=True)
        print (f"Created request queue: {self.rpc_send_queue_name}")
        
        #create queue for amps response - 
        self.service_bus_mgmt_client.create_queue(self.rpc_response_queue_name, max_delivery_count=10, dead_lettering_on_message_expiration=True)
        print (f"Created response queue: {self.rpc_send_queue_name}")
        
    def delete_queues(self):
        #delete queue for sharkfin daily values
        self.service_bus_mgmt_client.delete_queue(self.rpc_send_queue_name)
        print (f"Deleted request queue: {self.rpc_send_queue_name}")
        
        #delete used queue for amps response - 
        self.service_bus_mgmt_client.delete_queue(self.rpc_response_queue_name)
        print (f"Deleted response queue: {self.rpc_send_queue_name}")
        
    #method to instanciate a well-formed service bus message. requires passing json object as msg_body parameter
    def new_rpc_message(self,msg_body):
        msgdata = json.dumps(msg_body)
        self.service_bus_message = ServiceBusMessage(
        msgdata,
        session_id = self.rpc_session_id,
        reply_to = self.rpc_response_queue_name,
        reply_to_session_id = self.rpc_session_id,
        application_properties = {'finalMessage': 'no'})
        
    def new_finalrpc_message(self,msg_body):
        msgdata = json.dumps(msg_body)
        self.service_bus_message = ServiceBusMessage(
        msgdata,
        session_id = self.rpc_session_id,
        reply_to = self.rpc_response_queue_name,
        reply_to_session_id = self.rpc_session_id,
        application_properties = {'finalMessage': 'yes'})
        
    #method to send a service bus message
    def send_rpc_message(self):
        sender = self.service_bus_message_client.get_queue_sender(queue_name=self.rpc_send_queue_name)
        result = sender.send_messages(self.service_bus_message)
        #self.coorelation_id = result.correlation_id
        #self.message_id = result.message_id
        #print (f"Sent RPC message with ID: {self.message_id} to consumer queue {self.rpc_send_queue_name} await reply into response queue: {self.rpc_response_queue_name}...")
        print (f"Sent RPC message to request queue {self.rpc_send_queue_name}")
        print (f"Message ID: {self.service_bus_message.message_id}")
        print (f"Message Body: {self.service_bus_message}")
        print (f"Time to live: {self.service_bus_message.time_to_live}")
        print (f"Application Properties: {self.service_bus_message.application_properties}")
        print (f"Message ID: {self.service_bus_message.message_id}")
        print (f"Listening for response on queue: {self.rpc_response_queue_name}")
        return result
    
    def get_rpc_response(self, service_bus_client, qname, coorelation_id):
        receiver = service_bus_client.get_queue_receiver(queue_name=self.rpc_response_queue_name, max_wait_time=5)
        for msg in receiver:
            if self.coorelation_id == msg.correlation_id:
                self.response = body
        print("Received: " + str(msg))
        receiver.complete_message(msg)
        
    def get_rpc_connectionString(self,keyVaultName):
        keyVaultUri = (f"https://{keyVaultName}.vault.azure.net")
        credential = DefaultAzureCredential()
        client = SecretClient(vault_url=keyVaultUri, credential=credential)
        retrieved_secret_bus_conectionString = client.get_secret('sharkfinMQconnectionstring')
        sharkfinMQconnectionstring = retrieved_secret_bus_conectionString.value
        print('opened vault, obtained service bus connection string')
        return sharkfinMQconnectionstring
    
    def check_listener(self):
        listener = self.service_bus_message_client.get_queue_receiver(queue_name=self.rpc_response_queue_name)
        print('Starting Service Bus listener...waiting for message.')
        received_msgs = listener.receive_messages(max_message_count=1)
        for message in received_msgs:
            global msgBody
            print(f"Receiving Message ID: {message.message_id}")
            print(f"Coorolation ID: {message.correlation_id}")
            print(f"Message Body: {message}")
            print(f"Application Properties: {message.application_properties}")
            print(f"Delivery count: {message.delivery_count}")
            msgBody = str(message)
            #print(f"Received the follow message: {msgBody}")
            listener.complete_message(message)
            
    def complete_message(self):
        listener.complete_message(msg)
        
def rpc_unitTest(simulation_days, simID, kvnamespace):
    #instansiate the Market class
    mc= ClientRPCMarket(4,simID,kvnamespace)
    days = range(1,simulation_days+1)
    for day in days:
        print(f"Preparing Message {day}")
        #msgID = str(uuid.uuid4())
        #construct message data
        jsonmsgdata = {'seed': simID, 'bl': 11, 'sl': 12, 'end_simulation':False}
        #print(jsonmsgdata)
        #msgdata = json.dumps(jsonmsgdata)
        #print(msgdata)
        if day == len(days):
            sbMessage = mc.new_finalrpc_message(jsonmsgdata)
        else:
            sbMessage = mc.new_rpc_message(jsonmsgdata)
        sentMsg = mc.send_rpc_message()
        msgID = mc.service_bus_message.message_id
        print(f"Sent message to AMMPS. Message ID:{msgID}")
        fullmsgdata= {'day':day,'msgID':jsonmsgdata}
        fullmsgdata.update(jsonmsgdata)
        mc.sentMsgHistory.append(fullmsgdata)
        responseThread = threading.Thread(target=mc.check_listener())
        responseThread.start()                                       
        # waiting here for the response to be available in the response queue before continuing
        responseThread.join()                                          
        receivedMessage = msgBody
        print(f"Received response from AMMPS: {receivedMessage}")
        jsonreceivedMessage = json.loads(receivedMessage)
        respfullmsgdata= {'day':day,'msgID':'placeholdervalue'}
        respfullmsgdata.update(jsonreceivedMessage)
        mc.recvMsgHistory.append(respfullmsgdata)
    print(f"Printing list of received messages: {mc.recvMsgHistory}")
    print(f'Unit test complete. Cleaning up.')
    mc.delete_queues()
    return mc
