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
from azure.keyvault.secrets import SecretClient
from azure.identity import DefaultAzureCredential
from azure.data.tables import TableServiceClient
from azure.data.tables import TableClient
from azure.data.tables import UpdateMode


class simulationGrid():
    def __init__(self, gridTableName):
        self.table_name = gridTableName
        self.conn_str = get_azSecrect(vaultName,connectionName)
        self.table_service = TableServiceClient.from_connection_string(self.conn_str)
        self.table_client = self.table_service.get_table_client(self.table_name)
        self.simulations = list()
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
        retrieved_secret_string = "DefaultEndpointsProtocol=https;AccountName=sbsimulationdata;AccountKey=KG8uTvfQinDCQoJYycZ+PvB+jw5/ovAp7ZfPaMLaCU53wKtg4QThAJ2IowOqd60+tr32kLD96lkt+AStExWHNQ==;EndpointSuffix=core.windows.net"
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

    def startSingleSim(experimentName,simID):
        simulation = getSim(experimentName,simID)
        create_ammps_config_command = str(simulation['ammps_config_cmd'])
        start_ammps_command = str(simulation['start_ammps_cmd'])
        start_sharkfin_command = str(simulation['start_sharkfin_cmd'])
        currentStatus = simulation['status']
        ammpsdir = f"/shared/home/ammpssharkfin/output/ammps_test_{experimentName}{simID}out"
        currentStatus = 'pending'
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
            except Expection as e:
                print(f"failed to generate ammps config file - {e.message}")
                simulation['status'] = 'failed to create ammps configuration file'
                simulation['startTime'] = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
                update_table_entity(experimentName, simulation)
    #code to start ammps
            print(f"Starting AMMPS with: {simulation['start_ammps_cmd']}")
            try:
                ammps_out = open(f"output/logs/ammps/ammps_{experimentName}{simID}_out.log", "wb")
                ammps_err = open(f"output/logs/ammps/ammps_{experimentName}{simID}_err.log", "wb")
                start_ammps = subprocess.Popen(shlex.split(start_ammps_command), stdout=ammps_out, stderr=ammps_err,  universal_newlines=True)
                simulation['status'] = 'started ammps'
                simulation['startTime'] = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
                update_table_entity(experimentName, simulation)
                print(f"ammps_started successfully now waiting... -  {start_ammps.pid}")
                start_ammps.wait()
                print("after ammps.wait")
                time.sleep(60)
                compress_results(ammpsdir)
            except Exception as e:
                print(f"failed to start ammps -  {e.message}")
                simulation['status'] = 'failed to start AMMPS'
                simulation['startTime'] = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
                update_table_entity(experimentName, simulation)


            

vaultName = 'sharkfinkv'
connectionName = 'simulationdataConnectionString'
#experimentName = 'whiteShark10reps'

if __name__ == "__main__":
    experimentName = sys.argv[1]
    simid = sys.argv[2]
    vaultName = 'sharkfinkv'
    connectionName = 'simulationdataConnectionString'
    print(f"Attempting to start simulation {experimentName}_{simid}")
    startSingleSim(experimentName,simid)
    now = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
    print(f"slurmRunner.py completed at {now}")
