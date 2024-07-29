#!/usr/bin/env python3
from sharkfin.utilities import *
import numpy as np
import json
import uuid
import time
import os
import uuid
from azure.servicebus import ServiceBusClient, ServiceBusMessage
from azure.servicebus.management import ServiceBusAdministrationClient

class ClientRPCMarket():
	def __init__(self, seed_limit='', session_id='', host=''):
		self.simulation_price_scale = 1
		self.default_sim_price = 100
		seed_limit = None
		self.sample = 0
		self.seeds = []
		self.seed_limit = seed_limit
		self.latest_price = None
		self.prices = [self.default_sim_price]
		self.rpc_session_id = session_id
		self.rpc_send_queue_name = session_id +'sq'
		self.rpc_response_queue_name = session_id +'rq'
		self.rpc_host_name = host
		self.connection_str = host
		self.init_az_rpc()
		self.create_queues()
		
		
	#method to initialize Azure Service Bus Clients for messageing and managment
	def init_az_rpc(self):
		self.service_bus_mgmt_client = ServiceBusAdministrationClient.from_connection_string(self.connection_str)
		self.service_bus_message_client = ServiceBusClient.from_connection_string(conn_str=self.connection_str, logging_enable=True)
		
		
	#method to create the required sending and response queues for RPC pattern
	def create_queues(self):
		#create queue for sharkfin to send daily values
		self.service_bus_mgmt_client.create_queue(self.rpc_send_queue_name, max_delivery_count=10, dead_lettering_on_message_expiration=True)
		#create queue for amps response - 
		self.service_bus_mgmt_client.create_queue(self.rpc_response_queue_name, max_delivery_count=10, dead_lettering_on_message_expiration=True)
		
	#method to instanciate a well-formed service bus message. requires passing json object as msg_body parameter
	def new_rpc_message(self,msg_body):
		msgdata = json.dumps(msg_body)
		self.service_bus_message = ServiceBusMessage(
		msgdata,
		session_id = self.rpc_session_id,
		reply_to = self.rpc_response_queue_name,
		reply_to_session_id = self.rpc_session_id,
		application_properties = {'placeholdermetadata': 'custom_data_example_if_needed'})
		
	#method to send a service bus message
	def send_rpc_message(self):
		sender = self.service_bus_message_client.get_queue_sender(queue_name=self.rpc_send_queue_name)
		result = sender.send_messages(self.service_bus_message)
		#self.coorelation_id = result.correlation_id
		#self.message_id = result.message_id
		#print (f"Sent RPC message with ID: {self.message_id} to consumer queue {self.rpc_send_queue_name} await reply into response queue: {self.rpc_response_queue_name}...")
		print (f"Sent RPC message to consumer queue {self.rpc_send_queue_name} await reply into response queue: {self.rpc_response_queue_name}...")
		return result
	
	def get_rpc_response(self):
		receiver = self.service_bus_message_client.get_queue_receiver(queue_name=self.rpc_response_queue_name, max_wait_time=5)
		for msg in receiver:
			self.response = body
		print("Received: " + str(body))
		receiver.complete_message(msg)
		
		