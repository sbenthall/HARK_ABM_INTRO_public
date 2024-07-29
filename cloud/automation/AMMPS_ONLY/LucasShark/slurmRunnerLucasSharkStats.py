#!/usr/bin/env python3
import subprocess
import shlex
import shutil
import argparse
import time
import sys
import json
import uuid
import datetime
import os
import re
from azure.keyvault.secrets import SecretClient
from azure.identity import DefaultAzureCredential
from azure.data.tables import TableServiceClient
from azure.data.tables import TableClient
from azure.data.tables import UpdateMode
from azure.storage.blob import BlobServiceClient
from azure.core.exceptions import ResourceExistsError
from azure.storage.blob import BlobType



class simulationGrid():
    def __init__(self, gridTableName):
        connectionName = 'simulationdataConnectionString'
        vaultName = 'sharkfinkv'
        self.table_name = gridTableName
        self.conn_str = get_azSecrect(vaultName,connectionName)
        self.table_service = TableServiceClient.from_connection_string(self.conn_str)
        self.table_client = self.table_service.get_table_client(self.table_name)
        
def update_table_entity(experimentName, simEntity):
    simTable = simulationGrid(experimentName)
    resp = simTable.table_client.upsert_entity(mode=UpdateMode.MERGE, entity=simEntity)
    print(resp)

def get_azSecrect(keyVaultName, secrectName):
    keyVaultUri = (f"https://{keyVaultName}.vault.azure.net")
    #credential = DefaultAzureCredential()
    #client = SecretClient(vault_url=keyVaultUri, credential=credential)
    #retrieved_secret_obj = client.get_secret(secrectName)
    #retrieved_secret_string = retrieved_secret_obj.value
    retrieved_secret_string = CONN_STRING
    print('opened vault, retrieved secret string')
    return retrieved_secret_string

def getSim(experimentName, PartitionKey):
    simTable = simulationGrid(experimentName)
    parameters = {"pk": PartitionKey}
    name_filter = "PartitionKey eq @pk"
    sim = simTable.table_client.query_entities(query_filter=name_filter, parameters=parameters)
    for entity in sim:
        #print(entity)#['simid'])
        se = entity
        return se
    
def compress_results(directory):
    compression_command = "gzip -r " + directory
    compress_working_dir = subprocess.Popen(compression_command, shell=True)
    compress_working_dir.wait()

def upload_file_to_blob(file_path, blob_path, container_client):
    # Create a blob client for uploading the file
    blob_client = container_client.get_blob_client(blob_path)

    # Upload the file to the blob container
    with open(file_path, "rb") as file:
        blob_client.upload_blob(file, overwrite=True, blob_type=BlobType.BlockBlob)

def copy_files_to_blob(experimentName, simID,connection_string):
    simulation = getSim(experimentName,simID)
    currentStatus = simulation['status']
    print(currentStatus)
    simStatsSuffix =  '_sim_stats.txt'
    classStatsSuffix =  '_class_stats.csv'
    dataSuffix =  '_data.csv'
    histSuffix =  '_history.csv'
    tag = simulation['tag']
    container_name = experimentName
    simStatsFile = f'output/{experimentName}{simID}-{tag}{simStatsSuffix}'
    classStatsFile = f'output/{experimentName}{simID}-{tag}{classStatsSuffix}'
    dataFile = f'output/{experimentName}{simID}-{tag}{dataSuffix}'
    historyFile = f'output/{experimentName}{simID}-{tag}{histSuffix}'
    ammpsdataPath =  f'output/{experimentName}{simID}out'
    fileList = [simStatsFile,classStatsFile,dataFile,historyFile]
    try:
        # Create a blob service client using the provided connection string and credentials
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        # Get or create the blob container
        container_client = blob_service_client.get_container_client(container_name)
        try:
            container_client.create_container()
            print(f"Created blob container '{container_name}'.")
        except ResourceExistsError:
            print(f"Blob container '{container_name}' already exists.")
        
        try:
            for file in fileList:
                folder_path = os.getcwd()
                file_path = os.path.join(folder_path, file)
                blob_path = file_path.replace(folder_path, "").lstrip(os.path.sep)
                upload_file_to_blob(file_path, blob_path, container_client)
                
        #upload ammps directory
        try:
            print(f'Preparing to upload files from {ammpsdataPath}.')
            for root, _, files in os.walk(ammpsdataPath):
                print(f'{root}')
                for file in files:
                    file_path = os.path.join(root, file)
                    #blob_path = file_path.replace(folder_path, "")
                    blob_path = file_path.replace(ammpsdataPath, "").lstrip(os.path.sep)
                    print(f"PREPARING file '{file}' for transfer to blob container with path '{blob_path}'")
                    upload_file_to_blob(file_path, blob_path, container_client)
                    print(f"Uploaded file '{file}' to blob container with path '{blob_path}'")
            print("All files have been uploaded successfully.")
        except Exception as e:
            print(f"An error occurred: {str(e)}")
    except Exception as e:
        print(f"An error occurred: {str(e)}")    
    
def push_results(experimentName,simID):
    simulation = getSim(experimentName,simID)
    currentStatus = simulation['status']
    print(currentStatus)
    if currentStatus == 'Simulation Complete':
        try:
            simStatsSuffix =  '_sim_stats.txt'
            tag = simulation['tag']
            simStatsFile = f'output/{experimentName}{simID}-{tag}{simStatsSuffix}'
            with open(simStatsFile, 'r') as file:
                data = file.read()
                file.close()
            contents = json.loads(data)
            print(contents)
            for key in contents:
                print(f'Adding {key}:{contents[key]} to simulation Object')
                keyName = re.sub(r'\s+', '', key)
                keyName = re.sub(r'[^\w\s]', '', keyName)
                simulation[keyName] = contents[key]
            update_table_entity(experimentName, simulation)
            print(f'adding {simStatsFile} to table')
        except Exception as e:
            print(f"failed to update table storage -  {str(e)}")
    
def startSingleSim(experimentName,simID):
    simulation = getSim(experimentName,simID)
    create_ammps_config_command = str(simulation['ammps_config_cmd'])
    start_ammps_command = str(simulation['start_ammps_cmd'])
    start_sharkfin_command = str(simulation['start_sharkfin_cmd'])
    currentStatus = simulation['status']
    ammpsdir = f"/shared/home/ammpssharkfin/output/ammps_test_{experimentName}{simID}out"
    #currentStatus = 'pending'
    if currentStatus == 'pending':
        print(f"Building AMMPS config with: {simulation['ammps_config_cmd']}")
#code to build ammps config
        try:
            ammps_conf_out = open(f"output/logs/ammps_conf/ammps_conf{experimentName}{simID}_out.log", "wb")
            ammps_conf_err = open(f"output/logs/ammps_conf/ammps_conf{experimentName}{simID}_err.log", "wb")
            scwd = '/usr/simulation/ammps_config_generator/acg/simulations'
            create_ammps_config = subprocess.Popen(shlex.split(create_ammps_config_command), cwd=scwd, stdout=ammps_conf_out, stderr=ammps_conf_err, universal_newlines=True)
            create_ammps_config.wait() # Wait for config to be created
            print("Generation of AMMPS Configuration file Completed....Starting AMMPS")
            simulation['status'] = 'Successfully created ammps configuration file'
            simulation['timestamp'] = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
            update_table_entity(experimentName, simulation)
        except Exception as e:
            print(f"failed to generate ammps config file - {str(e)}")
            simulation['status'] = 'failed to create ammps configuration file'
            simulation['timestamp'] = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
            update_table_entity(experimentName, simulation)
#code to start ammps
        print(f"Starting AMMPS with: {simulation['start_ammps_cmd']}")
        try:
            ammps_out = open(f"output/logs/ammps/ammps_{experimentName}{simID}_out.log", "wb")
            ammps_err = open(f"output/logs/ammps/ammps_{experimentName}{simID}_err.log", "wb")
            start_ammps = subprocess.Popen(shlex.split(start_ammps_command), stdout=ammps_out, stderr=ammps_err,  universal_newlines=True)
            simulation['status'] = 'started ammps'
            simulation['timestamp'] = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
            update_table_entity(experimentName, simulation)
            print(f"AMMPS Started Successfully... awaiting SharkFin.... -  {start_ammps.pid}")
            time.sleep(5)
        except Exception as e:
            print(f"failed to start ammps -  {str(e)}")
            simulation['status'] = 'failed to start AMMPS'
            simulation['timestamp'] = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
            update_table_entity(experimentName, simulation)
#code to start sharkfin
        print(f"Starting Sharkfin with: {simulation['start_sharkfin_cmd']}")
        try:
            time.sleep(5)
            shark_out = open(f"output/logs/sharkfin/shark_{experimentName}{simID}_out.log", "wb")
            shark_err = open(f"output/logs/sharkfin/shark_{experimentName}{simID}_err.log", "wb")
            #start_sharkfin = subprocess.Popen(shlex.split(start_sharkfin_command), stdout=shark_out, stderr=shark_err, cwd ="/usr/simulation/SHARKFin/simulate/", universal_newlines=True)
            start_sharkfin = subprocess.Popen(start_sharkfin_command, stdout=shark_out, stderr=shark_err, cwd ="/usr/simulation/SHARKFin/simulate/", universal_newlines=True, shell=True)
            print(f"sharkfin started successfully {start_sharkfin.pid}")
            simulation['status'] = 'started sharkfin'
            simulation['timestamp'] = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
            update_table_entity(experimentName, simulation)
            print("beginging to wait for sharkfin")
            start_sharkfin.wait()
            print(f"sharkfin complete successfully {start_sharkfin.pid} Uploading Results to table storage.")
            simulation['status'] = 'Simulation Complete'
            simulation['endtime'] = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
            update_table_entity(experimentName, simulation)
            time.sleep(60)
        except Exception as e:
            print(f"failed to start sharkfin -  {str(e)}")
            simulation['status'] = 'Sharkfin failed to start'
            simulation['timestamp'] = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
            update_table_entity(experimentName, simulation)
        print("Updating table storage with simstats")
        currentStatus = simulation['status']

vaultName = 'sharkfinkv'
connectionName = 'simulationdataConnectionString'
CONN_STRING = "DefaultEndpointsProtocol=https;AccountName=sbsimulationdata;AccountKey=KG8uTvfQinDCQoJYycZ+PvB+jw5/ovAp7ZfPaMLaCU53wKtg4QThAJ2IowOqd60+tr32kLD96lkt+AStExWHNQ==;EndpointSuffix=core.windows.net"

if __name__ == "__main__":
    experimentName = sys.argv[1]
    simid = sys.argv[2]
    vaultName = 'sharkfinkv'
    connectionName = 'simulationdataConnectionString'
    print(f"Attempting to start simulation {experimentName}_{simid}")
    startSingleSim(experimentName,simid)
    push_results(experimentName,str(simid))
    copy_files_to_blob(experimentName,simid,CONN_STRING)
    now = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
    print(f"slurmRunner.py completed at {now}")