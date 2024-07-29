import json
import datetime
import os
import jwt
import seaborn as sns
import uuid
import time
import yaml
import pandas as pd
import numpy as np
import math
import matplotlib.pyplot as plt
import subprocess
import shlex
import shutil
import argparse
import io
import paramiko
import zipfile
from paramiko import SSHClient
from paramiko import RSAKey
from paramiko import AutoAddPolicy
from scp import SCPClient
from pprint import pprint as pp
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from azure.keyvault.keys import KeyClient
from azure.keyvault.secrets import SecretClient
from azure.identity import DefaultAzureCredential
from azure.identity import InteractiveBrowserCredential
from azure.data.tables import TableServiceClient
from azure.data.tables import TableClient
from azure.data.tables import UpdateMode
from base64 import urlsafe_b64encode
from cryptography.hazmat.primitives import serialization
import matplotlib.pyplot as plt
import pandas as pd

#global vaultName
#global connectionName

class simulationGrid: 
    def __init__(self, gridTableName, vaultName, connectionName):
        self.table_name = gridTableName
        self.conn_str = get_azSecrect(vaultName, connectionName)
        self.table_service = TableServiceClient.from_connection_string(self.conn_str)
        self.table_client = self.table_service.get_table_client(self.table_name)
        self.simulations = self.table_client.list_entities()

class simEntity:
    def __init__(self, entity):
        self.PartitionKey = entity["PartitionKey"]
        self.RowKey = entity["RowKey"]
        self.simid = entity["simid"]
        self.attention = entity["attention"]
        self.dividend_growth_rate = entity["dividend_growth_rate"]
        self.dphm = entity["dphm"]
        self.p1 = entity["p1"]
        self.p2 = entity["p2"]
        self.d1 = entity["d1"]
        self.d2 = entity["d2"]
        self.popn = entity["popn"]
        self.quarters = entity["quarters"]
        self.runs = entity["runs"]
        self.seed = entity["seed"]
        self.startTime = ""
        self.endTime = ""
        self.runTime = ""
        self.seed = entity["seed"]
        self.sharkfin_saveAs = entity["sharkfin"]["save_as"]
        self.sharkfin_parameters = entity["sharkfin"]["parameters"]
        self.ammps_config_file_name = entity["ammps_config_gen"][
            "ammps_config_file_name"
        ]
        self.ammps_ammps_output_dir = entity["ammps_config_gen"]["ammps_output_dir"]
        self.ammps_config_parameters = entity["ammps_config_gen"]["parameters"]
        self.ammps_parameters = entity["ammps"]["parameters"]


def get_azSecrect(keyVaultName, secrectName):
        keyVaultUri = (f"https://{keyVaultName}.vault.azure.net")
        credential = DefaultAzureCredential()
        client = SecretClient(vault_url=keyVaultUri, credential=credential)
        retrieved_secret_obj = client.get_secret(secrectName)
        retrieved_secret_string = retrieved_secret_obj.value
        print('opened vault, retrived secrect string')
        return retrieved_secret_string
    

    
def get_azKey(keyVaultName, keyName):
        keyVaultUri = (f"https://{keyVaultName}.vault.azure.net")
        credential = DefaultAzureCredential()
        client = KeyClient(vault_url=keyVaultUri, credential=credential)
        retrieved_key_obj = client.get_key(keyName)
        usable_jwk = {}
        for k in vars(retrieved_key_obj.key):
            value = vars(retrieved_key_obj.key)[k]
            if value:
                usable_jwk[k] = urlsafe_b64encode(value) if isinstance(value, bytes) else value
        public_key = jwt.algorithms.RSAAlgorithm.from_jwk(usable_jwk)
        public_pem = public_key.public_bytes(encoding=serialization.Encoding.PEM,format=serialization.PublicFormat.SubjectPublicKeyInfo)
        # Open file in binary write mode
        binary_file = open(f"{keyName}.pem", "wb")
        # Write bytes to file
        binary_file.write(public_pem)
        # Close file
        binary_file.close()
        print(f"Key with name '{retrieved_key_obj.name}' and saved as '{keyName}.pem'")
        return public_pem

def create_table(tableName, vaultName, connectionName):
    conn_str = get_azSecrect(vaultName, connectionName)
    print('Creating Table Client')
    table_service_client = TableServiceClient.from_connection_string(conn_str)
    print('Creating Table')
    table_client = table_service_client.create_table(table_name=tableName)
    return table_client


def create_table_entity(parameterGrid, simEntity):
    # print(type(simEntity['ammps_config_gen']['parameters']))
    # simEntity['ammps_config_gen']['parameters'] = json.dumps(simEntity['ammps_config_gen']['parameters'])
    # simEntity['ammps_config_gen'] = json.dumps(simEntity['ammps_config_gen'])
    # simEntity['ammps']['parameters'] = json.dumps(simEntity['ammps']['parameters'])
    # simEntity['ammps'] = json.dumps(simEntity['ammps'])
    # simEntity['sharkfin']['parameters'] = json.dumps(simEntity['sharkfin']['parameters'])
    # simEntity['sharkfin'] = json.dumps(simEntity['sharkfin'])
    # print(type(simEntity['ammps_config_gen']['parameters']))
    # resp = parameterGrid.table_client.create_entity(entity=simEntity)
    
    resp = parameterGrid.table_client.upsert_entity(
        mode=UpdateMode.MERGE, entity=simEntity
    )
    print(resp)


def update_table_entity(parameterGrid, simEntity):
    resp = parameterGrid.table_client.upsert_entity(
        mode=UpdateMode.MERGE, entity=simEntity
    )
    print(resp)


def get_entities(parameterGrid):
    entities = parameterGrid.table_client.list_entities()
    sims = list(entities)
    for entity in sims:
        entity["ammps_config_gen"] = json.loads(entity["ammps_config_gen"])
        entity["ammps_config_gen"]["parameters"] = json.loads(
            entity["ammps_config_gen"]["parameters"]
        )
        entity["ammps"] = json.loads(entity["ammps"])
        entity["ammps"]["parameters"] = json.loads(entity["ammps"]["parameters"])
        entity["sharkfin"] = json.loads(entity["sharkfin"])
        entity["sharkfin"]["parameters"] = json.loads(entity["sharkfin"]["parameters"])
    return sims


def getSim(experimentName, PartitionKey):
    vaultName = 'sharkfinkv'
    connectionName = 'simulationdataConnectionString'
    simTable = simulationGrid(experimentName, vaultName, connectionName)
    parameters = {"pk": PartitionKey}
    name_filter = "PartitionKey eq @pk"
    sim = simTable.table_client.query_entities(
        query_filter=name_filter, parameters=parameters
    )
    for entity in sim:
        # print(entity)#['simid'])
        se = entity
        return se

def getSchedulerStatus(schedulerIP, clusterKey, user):
    cmd =  'pwd'
    scmd = f"ssh -o StrictHostKeyChecking=no -i {clusterKey} {user}@{schedulerIP} {cmd}"
    out, err = subprocess.Popen(scmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True).communicate()
    if out == '/shared/home/ammpssharkfin\n':
        print(f"Attempting to connect to {schedulerIP}.......")
        print("Connected")
        cmd='squeue | wc -l'
        scmd = f"ssh -i {clusterKey} {user}@{schedulerIP} {cmd}"
        out, err = subprocess.Popen(scmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True).communicate()
        print(f"There are {int(out)-1} items in the queue.")
        cmd='sinfo'
        scmd = f"ssh -i {clusterKey} {user}@{schedulerIP} {cmd}"
        print(scmd)
        #out, err = subprocess.Popen(scmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True).communicate()
        print(out)
        print(f"Scheduler {schedulerIP} is ready.")
    if err:
        print(err)
    
def runSSH(hostname, clusterKey, user, command):
    cmd =  command
    scmd = f"ssh -o StrictHostKeyChecking=no -i {clusterKey} {user}@{hostname} {cmd}"
    out, err = subprocess.Popen(scmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True).communicate()
    if out:
        print(f"Attempting to connect to {hostname}.......")
        print("Connected")
        print(out)
        return out
    if err:
        print(err)
        return err

def submit_file_to_remote(hostname, user, sshkeypath, localpath, remotepath, execute=False):
    # Create an SSH client
    ssh = SSHClient()

    # Automatically add the remote host key
    ssh.set_missing_host_key_policy(AutoAddPolicy())

    # Load the SSH private key
    private_key = RSAKey.from_private_key_file(sshkeypath)

    # Connect to the remote machine
    ssh.connect(hostname=hostname, username=user, pkey=private_key)

    # Create an SCP client
    scp = SCPClient(ssh.get_transport())

    # Copy the file
    scp.put(localpath, remotepath)

    # Close the SCP client
    scp.close()
    filename = localpath
    expath = f"{remotepath}"
    if execute:
        # Execute the file on the remote machine using sbatch
        stdin, stdout, stderr = ssh.exec_command(f'chmod +x {expath} && sbatch {expath}')

        # Read the output and error streams
        output = stdout.read().decode()
        error = stderr.read().decode()

        # Close the input, output, and error streams
        stdin.close()
        stdout.close()
        stderr.close()

        # Return the output and error
        return output, error

    # Close the SSH connection
    ssh.close()


def list_remoteDir(hostname, user, sshkeypath, remotepath):
    # Create an SSH client
    ssh = SSHClient()

    # Automatically add the remote host key
    ssh.set_missing_host_key_policy(AutoAddPolicy())

    # Load the SSH private key
    private_key = RSAKey.from_private_key_file(sshkeypath)

    # Connect to the remote machine
    ssh.connect(hostname=hostname, username=user, pkey=private_key)

    # command to execute on client
    stdin, stdout, stderr = ssh.exec_command(f'sudo -i && ls -l {expath}')
    # Read the output and error streams
    output = stdout.read().decode()
    error = stderr.read().decode()

    # Close the input, output, and error streams
    stdin.close()
    stdout.close()
    stderr.close()
    # Return the output and error
    return output, error

    # Close the SSH connection
    ssh.close()
    
    
    
    
def copy_file_from_remote(hostname, user, sshkeypath, localpath, remotepath):
    # Create an SSH client
    ssh = SSHClient()
    ssh.load_system_host_keys()
    ssh.set_missing_host_key_policy(AutoAddPolicy())

    # Load the SSH private key
    private_key = RSAKey.from_private_key_file(sshkeypath)

    try:
        # Connect to the remote machine
        ssh.connect(hostname=hostname, username=user, pkey=private_key)

        # Create an SCP client
        scp = SCPClient(ssh.get_transport())

        # Copy the file
        scp.get(remotepath, localpath)

        # Close the SCP client
        scp.close()

        # Close the SSH connection
        ssh.close()
        
    except SSHException as e:
        print(f"Error: {e}")
    
    
    
def generate_slurm_job_depricated(experiment_name, array_length):
    job_name = f"{experiment_name}_Slurm_Job"
    nodes = 1
    ntasks_per_node = 1
    cpus_per_task = 1
    time = "24:00:00"
    output_dir = "./output/logs/slurm"
    output_file_pattern = f"{output_dir}/{experiment_name}_%A_%a.out.log"
    command = f"python3 slurmRunner.py {experiment_name} $SLURM_ARRAY_TASK_ID"
    slurm_args = [
        f"--job-name={job_name}",
        f"--nodes={nodes}",
        f"--ntasks-per-node={ntasks_per_node}",
        f"--cpus-per-task={cpus_per_task}",
        f"--array=1-{array_length}",
        f"--time={time}",
        f"--output={output_file_pattern}",
    ]
    sbatch_command = ["sbatch"] + slurm_args + [command]
    return(sbatch_command)
    #print(sbatch_command)
    

    
def getSharkfinStatus(schedulerIP,simID, clusterKey, user):
    ammpsLog = f"ammps_{simID}out.log"
    cmd= f"tail ./output/logs/ammps/{ammpsLog}"
    scmd = f"ssh -o StrictHostKeyChecking=no -i {clusterKey} {user}@{schedulerIP} {cmd}"
    out, err = subprocess.Popen(scmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True).communicate()
    if out:
        print(f"Attempting to connect to {schedulerIP}.......")
        print("Connected")
        print(out)
    if err:
        print(err)
        
    
    
def startAllSims(experimentName):
    simgrid = simulationGrid(experimentName)
    entities = simgrid.table_client.list_entities()
    tableEntities = list(entities)
    for e in tableEntities:
        currentStatus = e["status"]
        print(currentStatus)
        if currentStatus == "pending":
            # code to build ammps_config_file
            print(e["ammps_config_cmd"])
            # code to start ammps
            print(e["start_ammps_cmd"])
            # code to start sharkfin
            print(e["start_sharkfin_cmd"])
            e["status"] = "started"
            e["startTime"] = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
            update_table_entity(simgrid, e)


def startSingleSim(experimentName, simID):
    e = getSim(experimentName, simID)
    currentStatus = e["status"]
    print(currentStatus)
    if currentStatus == "pending":
        # code to build ammps_config_file
        print(e["ammps_config_cmd"])
        # code to start ammps
        print(e["start_ammps_cmd"])
        # code to start sharkfin
        print(e["start_sharkfin_cmd"])
        e["status"] = "started"
        e["startTime"] = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
        update_table_entity(simgrid, e)


def stopAllSims(experimentName):
    simgrid = simulationGrid(experimentName)
    entities = simgrid.table_client.list_entities()
    tableEntities = list(entities)
    for e in tableEntities:
        currentStatus = e["status"]
        print(currentStatus)
        if currentStatus == "started sharkfin":
            # code to build ammps_config_file
            print(e["ammps_config_cmd"])
            # code to start ammps
            print(e["start_ammps_cmd"])
            # code to start sharkfin
            print(e["start_sharkfin_cmd"])
            e["status"] = "pending"
            e["startTime"] = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
            update_table_entity(simgrid, e)


def dict_to_args(input_dict):
    string = " "
    for k in input_dict:
        v = input_dict[k]
        string += "--{} {} ".format(str(k), str(v))
    return string

def generate_slurm_job(experiment_name, array_length):
    job_name = f"{experiment_name}_Slurm_Job"
    nodes = 1
    ntasks_per_node = 1
    cpus_per_task = 1
    array_length = f"1-{array_length}"
    timeout = "3:00:00"

    job_script = f"""#!/bin/bash
#SBATCH --job-name={job_name}
#SBATCH --nodes={nodes}
#SBATCH --ntasks-per-node={ntasks_per_node}
#SBATCH --cpus-per-task={cpus_per_task}
#SBATCH --array={array_length}
#SBATCH --time={timeout}
#SBATCH --partition=htc
#SBATCH --output=./output/logs/slurm/{experiment_name}job_%A_%a.out.log
#SBATCH --error=./output/logs/slurm/{experiment_name}job_%A_%a.err.log
echo "Start of bash script. Setting environmental variables."
date -u
SIMID=$SLURM_ARRAY_TASK_ID
EXPERIMENTNAME={experiment_name}
echo "Staring simulation $SIMID plese wait."
date -u
python3.9 /shared/home/ammpssharkfin/slurmRunner.py $EXPERIMENTNAME $SIMID
wait
echo "slurmRunner.py has returned! Bash script exiting."
date -u"""

    # Write the job script to a file
    with open(f'{experiment_name}job.sh', 'w') as f:
        f.write(job_script)
    # Submit the job using sbatch
    shellscript = job_script
    #subprocess.run(['sbatch', f'{experiment_name}job.sh'])
    #runSSH(hostname, clusterKey, user, command)
    return(shellscript)
    #print(sbatch_command)
    #subprocess.run(combined_command)




def get_results(simid):
    simcmd = f"tail ./allSharkfinrep10data/whiteShark10reps{simid}-whiteShark10reps_sim_stats.txt"
    classcmd = f"tail ./allSharkfinrep10data/whiteShark10reps{simid}-whiteShark10reps_class_stats.csv"
    # cmd= 'squeue | wc -l'
    scmd = f"ssh -i {key} {user}@{host} {simcmd}"
    ccmd = f"ssh -i {key} {user}@{host} {classcmd}"
    c1out, c1err = subprocess.Popen(
        scmd,
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        encoding="utf-8",
    ).communicate()
    c2out, c2err = subprocess.Popen(
        ccmd,
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        encoding="utf-8",
    ).communicate()
    # self.simstats = c1out
    # self.classtats = c2out
    simstats = c1out
    classtats = c2out
    return simstats, classtats


def start_experiment(experimentName, array=800 - 999):
    host = "20.163.190.207"
    user = "ammpssharkfin"
    cmd = "tail ./output/logs/ammps/ammps_whiteShark10reps110_out.log"
    key = "/Users/wjt5121/ssh/cycleShark_key.pem"
    runnercmd = f"""
	date -u 
	SIMID=$SLURM_ARRAY_TASK_ID
	EXPERIMENTNAME="{experimentName}"
	echo "Executing simulation $SIM"
	date -u
	srun --wait python3 slurmRunner.py $EXPERIMENTNAME $SIMID
	echo "slurmRunner.py has returned!"
	date -u"""

    #sbatchcmd = f"sbatch --nodes=1 --ntasks-per-node=1 --cpus-per-task=1 --time=12:00:00 --output=./output/logs/slurm/wsjob_%A_%a.out.log --array={array} --wrap {runnercmd}"
    cmd = f"ssh -i {key} {user}@{host} {simcmd}"
    sbout, sberr = subprocess.Popen(
        scmd,
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        encoding="utf-8",
    ).communicate()
    return sbout, sberr


def create_container(experimentName):
    try:
        print("Attempting connection to storage account using Blob Service Client")
        # connect_str = os.getenv('AZURE_STORAGE_CONNECTION_STRING')
        # todo this is hard coded connection string. move this to the keyvault and add code to pull it out
        connect_str = "DefaultEndpointsProtocol=https;AccountName=sbsimulationdata;AccountKey=KR3zmvCUXKMgW6TZwdUhnfPBGdOr65p24cMvLW1vB6IIp3A18ikgIOit11eQHTjcEDA6KHLglPIz+AStqYXzJQ==;EndpointSuffix=core.windows.net"
        # Create the BlobServiceClient object
        blob_service_client = BlobServiceClient.from_connection_string(connect_str)
        # Quickstart code goes here
    except Exception as ex:
        print("Exception:")
        print(ex)
    try:
        # Create a unique name for the container
        container_name = f"{experimentName}"
        # Create the container
        container_client = blob_service_client.create_container(container_name.lower())
        print(f"Created container {container_name}")
    except Exception as ex:
        print("Exception:")
        print(ex)


def upload_blob(container, file, index=0, result=None):
    if result is None:
        result = [None]
    try:
        # extract blob name from file path
        blob_name = "".join(os.path.splitext(os.path.basename(file)))
        # connect_str = os.getenv('AZURE_STORAGE_CONNECTION_STRING')
        blob = BlobClient.from_connection_string(
            conn_str=connect_str, container_name=container.lower(), blob_name=blob_name
        )
        with open(file, "rb") as data:
            blob.upload_blob(data, overwrite=True)
        print(f"Upload succeeded: {blob_name}")
        result[index] = True  # example of returning result
    except Exception as e:
        print(e)  # do something useful here
        result[index] = False  # example of returning result


def upload_directory(dirpath, container):
    local_path = dirpath
    for file in os.listdir(local_path):
        if os.path.isfile(file) == True:
            upload_blob(container, file)
            print(f"Uploaded:{file} to {container}")


def list_blobs_in_container(container):
    blobs = []
    connect_str = "DefaultEndpointsProtocol=https;AccountName=sbsimulationdata;AccountKey=KR3zmvCUXKMgW6TZwdUhnfPBGdOr65p24cMvLW1vB6IIp3A18ikgIOit11eQHTjcEDA6KHLglPIz+AStqYXzJQ==;EndpointSuffix=core.windows.net"
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)
    container_client = blob_service_client.get_container_client(container)
    blobs_list = container_client.list_blobs()
    for blob in blobs_list:
        if blob.name != "p2data":
            print(blob.name + "\n")
            blobs.append(blob)
    return blobs_list


def download_all_blobs_in_container(container, path):
    connect_str = "DefaultEndpointsProtocol=https;AccountName=sbsimulationdata;AccountKey=KR3zmvCUXKMgW6TZwdUhnfPBGdOr65p24cMvLW1vB6IIp3A18ikgIOit11eQHTjcEDA6KHLglPIz+AStqYXzJQ==;EndpointSuffix=core.windows.net"
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)
    container_client = blob_service_client.get_container_client(container)
    blobs_list = container_client.list_blobs()
    for blob in blobs_list:
        if blob.name != container:
            head, tail = os.path.split("{}".format(blob.name))
            print(blob.name)
            print(head)
            print(tail)
            data = container_client.get_blob_client(blob).download_blob().readall()
            save_blob_locally(blob.name, data)

def save_blob_locally(name, data):
    file_name = name
    download_file_path = os.path.join(localBlobPath, file_name)

    with open(download_file_path, "wb") as file:
        file.write(data)
    return file_name

def build_any_configs(
    experimentName,
    seedcount,
    seedKey=6174,
    rpcHost="20.230.0.191",
    dphms=[10000],
    p1_values=[0.01],
    attention_values=[0.05],
    dividend_growth_rates=[1.0015],
    p2_values=[0.99],
    quarters=2,
    runs=30,
    popn=25,
    tag="r2"
):
    # entity_generation_version2 all feilds reproduced flat
    # experimentName = sandShark
    # number_of_repeats = 25 #number of seeds
    # rpc_host = "20.230.0.191" # "sharkfinmq.azure.microsoft.net"
    echo = shutil.which("echo")
    sleep = shutil.which("sleep")
    python = shutil.which("python3")
    dotnet = shutil.which("dotnet")
    cp = shutil.which("cp")
    gzip = shutil.which("gzip")
    np.random.seed(seedKey)  # Set seed for reprodusability
    number_of_repeats = seedcount
    rnd_seeds = np.random.randint(0, high=100000, size=number_of_repeats)
    print(f"Generating simulations using the following seeds:{rnd_seeds}")
    days_to_simulate = 480
    # rpc_host = "sharkfinmq.azure.microsoft.net"
    rpc_host = rpcHost
    # attention_values = [0.05, 0.5, 0.95]
    # p1_values = [0.1]
    # p2_values = [0.1]
    # dividend_growth_rates = [1.00, 1.002]
    # dphms = [1000, 35000, 70000]
    # quarters = 2
    # runs = 60
    # popn = 25
    # tag = "slurm_babyWhiteSharkrun4"
    attention_values = attention_values
    p1_values = p1_values
    p2_values = p2_values
    dividend_growth_rates = dividend_growth_rates
    dphms = dphms
    quarters = quarters
    runs = runs
    popn = popn
    tag = tag

    n = 1
    n_files = 1
    configs = list()

    for attention_value in attention_values:
        for p1_value in p1_values:
            for p2_value in p2_values:
                for dividend_growth_rate in dividend_growth_rates:
                    for dphm in dphms:
                        for seed in rnd_seeds:
                            # pkey = (str(n)).zfill(5) #con use zfill to pad leading zeros is needed.
                            pkey = str(n)
                            simDict = {
                                "PartitionKey": pkey,
                                "RowKey": experimentName
                                + str(n)
                                + "|"
                                + str(seed)
                                + "|"
                                + "Attention"
                                + "|"
                                + str(attention_value)
                                + "|"
                                + str(p1_value)
                                + "|"
                                + str(dividend_growth_rate)
                                + "|"
                                + str(dphm),
                                "experimentName": experimentName,
                                "status": "pending",
                                "simid": n,
                                "attention": attention_value,
                                "dividend_growth_rate": dividend_growth_rate,
                                "dphm": dphm,
                                "p1": p1_value,
                                "p2": p2_value,
                                "d1": 0.1,
                                "d2": 0.1,
                                "popn": 25,
                                "quarters": quarters,
                                "runs": runs,
                                "tag": tag,
                                "ammps_config_cmd": "",
                                "start_ammps_cmd": "",
                                "start_sharkfin_cmd": "",
                                "seed": str(seed),
                                "sharkfin": {
                                    "save_as": "/shared/home/ammpssharkfin/output/"
                                    + experimentName
                                    + str(n),
                                    # "save_as" : "../../output/" + experimentName + str(n),
                                    "parameters": {
                                        "simulation": "Attention",
                                        "tag": tag,
                                        "seed": str(seed),
                                        "popn": popn,
                                        "quarters": quarters,
                                        "runs": runs,
                                        "attention": attention_value,
                                        "dphm": dphm,
                                        "market": "ClientRPCMarket",
                                        "dividend_growth_rate": dividend_growth_rate,  # just vary this  dr_rate_return_values? 1.0, 1.002
                                        "dividend_std": 0.011988,  # look at paper
                                        "queue": experimentName + str(n),
                                        "rhost": rpc_host,
                                        "p1": p1_value,
                                        "p2": p2_value,
                                        "d1": 0.1,  # constnt, look at paper
                                        "d2": 0.1,  # constant
                                    },
                                },
                                "ammps": {
                                    "ammps_config_file_name": "test_conf"
                                    + str(n)
                                    + ".xlsx",
                                    "ammps_output_dir": "/shared/home/ammpssharkfin/output/ammps_data_"
                                    + experimentName
                                    + str(n)
                                    + "out",
                                    "parameters": {
                                        "number": 0,
                                        "rabbitMQ-host": rpc_host,
                                        "rabbitMQ-queue": experimentName + str(n),
                                        "t": "true",
                                    },
                                },
                                "ammps_config_gen": {
                                    "parameters": {
                                        "seed": str(seed),
                                        "name": "gg_test_conf" + str(n) + ".xlsx",
                                        "days": quarters * 60,
                                        "out-dir": "/usr/simulation/",
                                    }
                                },
                            }
                            simDict["ammps_config_cmd"] = (
                                "/usr/bin/python3 /usr/simulation/ammps_config_generator/make_single_config.py"
                                + dict_to_args(
                                    simDict["ammps_config_gen"]["parameters"]
                                )
                            )
                            simDict["start_ammps_cmd"] = (
                                "/usr/bin/dotnet /usr/simulation/ammps_bin/amm.engine.dll RunConfFromFile /usr/simulation/"
                                + simDict["ammps"]["ammps_config_file_name"]
                                + " "
                                + simDict["ammps"]["ammps_output_dir"]
                                + dict_to_args(simDict["ammps"]["parameters"])
                            )
                            simDict["start_sharkfin_cmd"] = (
                                "/usr/bin/python3 /usr/simulation/SHARKFin/simulate/run_any_simulation.py "
                                + simDict["sharkfin"]["save_as"]
                                + dict_to_args(simDict["sharkfin"]["parameters"])
                            )
                            simDict["sharkfin"]["parameters"] = json.dumps(
                                simDict["sharkfin"]["parameters"]
                            )
                            simDict["sharkfin"] = json.dumps(simDict["sharkfin"])
                            simDict["ammps"]["parameters"] = json.dumps(
                                simDict["ammps"]["parameters"]
                            )
                            simDict["ammps"] = json.dumps(simDict["ammps"])
                            simDict["ammps_config_gen"]["parameters"] = json.dumps(
                                simDict["ammps_config_gen"]["parameters"]
                            )
                            simDict["ammps_config_gen"] = json.dumps(
                                simDict["ammps_config_gen"]
                            )
                            simDict[
                                "cmdBundle"
                            ] = f"{simDict['ammps_config_cmd']}{simDict['start_ammps_cmd']}{simDict['start_sharkfin_cmd']}"
                            configs.append(simDict)
                            n += 1
    return configs


records_row = 0
allclean_row = 0



def build_ggshark_configs(
    experimentName,
    seedcount,
    seedKey=6174,
    rpcHost="20.230.0.191",
    dphms=[10000],
    p1_values=[0.01],
    attention_values=[0.05],
    dividend_growth_rates=[1.0015],
    p2_values=[0.99],
    quarters=2,
    runs=30,
    popn=25,
    tag="r2",
):
    # entity_generation_version2 all feilds reproduced flat
    # experimentName = sandShark
    # number_of_repeats = 25 #number of seeds
    # rpc_host = "20.230.0.191" # "sharkfinmq.azure.microsoft.net"
    echo = shutil.which("echo")
    sleep = shutil.which("sleep")
    python = shutil.which("python3")
    dotnet = shutil.which("dotnet")
    cp = shutil.which("cp")
    gzip = shutil.which("gzip")
    np.random.seed(seedKey)  # Set seed for reprodusability
    number_of_repeats = seedcount
    rnd_seeds = np.random.randint(0, high=100000, size=number_of_repeats)
    print(f"Generating simulations using the following seeds:{rnd_seeds}")
    days_to_simulate = 480
    # rpc_host = "sharkfinmq.azure.microsoft.net"
    rpc_host = rpcHost
    # attention_values = [0.05, 0.5, 0.95]
    # p1_values = [0.1]
    # p2_values = [0.1]
    # dividend_growth_rates = [1.00, 1.002]
    # dphms = [1000, 35000, 70000]
    # quarters = 2
    # runs = 60
    # popn = 25
    # tag = "slurm_babyWhiteSharkrun4"
    attention_values = attention_values
    p1_values = p1_values
    p2_values = p2_values
    dividend_growth_rates = dividend_growth_rates
    dphms = dphms
    quarters = quarters
    runs = runs
    popn = popn
    tag = tag

    n = 1
    n_files = 1
    configs = list()

    for attention_value in attention_values:
        for p1_value in p1_values:
            for p2_value in p2_values:
                for dividend_growth_rate in dividend_growth_rates:
                    for dphm in dphms:
                        for seed in rnd_seeds:
                            # pkey = (str(n)).zfill(5) #con use zfill to pad leading zeros is needed.
                            pkey = str(n)
                            simDict = {
                                "PartitionKey": pkey,
                                "RowKey": experimentName
                                + str(n)
                                + "|"
                                + str(seed)
                                + "|"
                                + "Attention"
                                + "|"
                                + str(attention_value)
                                + "|"
                                + str(p1_value)
                                + "|"
                                + str(dividend_growth_rate)
                                + "|"
                                + str(dphm),
                                "experimentName": experimentName,
                                "status": "pending",
                                "simid": n,
                                "attention": attention_value,
                                "dividend_growth_rate": dividend_growth_rate,
                                "dphm": dphm,
                                "p1": p1_value,
                                "p2": p2_value,
                                "d1": 0.1,
                                "d2": 0.1,
                                "popn": 25,
                                "quarters": quarters,
                                "runs": runs,
                                "tag": tag,
                                "ammps_config_cmd": "",
                                "start_ammps_cmd": "",
                                "start_sharkfin_cmd": "",
                                "seed": str(seed),
                                "sharkfin": {
                                    "save_as": "/shared/home/ammpssharkfin/output/"
                                    + experimentName
                                    + str(n),
                                    # "save_as" : "../../output/" + experimentName + str(n),
                                    "parameters": {
                                        "simulation": "Attention",
                                        "tag": tag,
                                        "seed": str(seed),
                                        "popn": popn,
                                        "quarters": quarters,
                                        "runs": runs,
                                        "attention": attention_value,
                                        "dphm": dphm,
                                        "market": "ClientRPCMarket",
                                        "dividend_growth_rate": dividend_growth_rate,  # just vary this  dr_rate_return_values? 1.0, 1.002
                                        "dividend_std": 0.011988,  # look at paper
                                        "queue": experimentName + str(n),
                                        "rhost": rpc_host,
                                        "p1": p1_value,
                                        "p2": p2_value,
                                        "d1": 0.1,  # constnt, look at paper
                                        "d2": 0.1,  # constant
                                    },
                                },
                                "ammps": {
                                    "ammps_config_file_name": "test_conf"
                                    + str(n)
                                    + ".xlsx",
                                    "ammps_output_dir": "/shared/home/ammpssharkfin/output/ammps_data_"
                                    + experimentName
                                    + str(n)
                                    + "out",
                                    "parameters": {
                                        "number": 0,
                                        "rabbitMQ-host": rpc_host,
                                        "rabbitMQ-queue": experimentName + str(n),
                                        "t": "true",
                                    },
                                },
                                "ammps_config_gen": {
                                    "parameters": {
                                        "seed": str(seed),
                                        "name": "test_conf" + str(n) + ".xlsx",
                                        "days": quarters * 60,
                                        "out-dir": "/usr/simulation/",
                                    }
                                },
                            }
                            simDict["ammps_config_cmd"] = (
                                "/usr/bin/python3 /usr/simulation/ammps_config_generator/make_ggshark_config.py"
                                + dict_to_args(
                                    simDict["ammps_config_gen"]["parameters"]
                                )
                            )
                            simDict["start_ammps_cmd"] = (
                                "/usr/bin/dotnet /usr/simulation/ammps_bin/amm.engine.dll RunConfFromFile /usr/simulation/"
                                + simDict["ammps"]["ammps_config_file_name"]
                                + " "
                                + simDict["ammps"]["ammps_output_dir"]
                                + dict_to_args(simDict["ammps"]["parameters"])
                            )
                            simDict["start_sharkfin_cmd"] = (
                                "/usr/bin/python3 /usr/simulation/SHARKFin/simulate/run_any_simulation.py "
                                + simDict["sharkfin"]["save_as"]
                                + dict_to_args(simDict["sharkfin"]["parameters"])
                            )
                            simDict["sharkfin"]["parameters"] = json.dumps(
                                simDict["sharkfin"]["parameters"]
                            )
                            simDict["sharkfin"] = json.dumps(simDict["sharkfin"])
                            simDict["ammps"]["parameters"] = json.dumps(
                                simDict["ammps"]["parameters"]
                            )
                            simDict["ammps"] = json.dumps(simDict["ammps"])
                            simDict["ammps_config_gen"]["parameters"] = json.dumps(
                                simDict["ammps_config_gen"]["parameters"]
                            )
                            simDict["ammps_config_gen"] = json.dumps(
                                simDict["ammps_config_gen"]
                            )
                            simDict[
                                "cmdBundle"
                            ] = f"{simDict['ammps_config_cmd']}{simDict['start_ammps_cmd']}{simDict['start_sharkfin_cmd']}"
                            configs.append(simDict)
                            n += 1
    return configs


def build_ammps_only_configs(
    experimentName,
    seedcount,
    seedKey,
    market_fraction_values,
    ammps_volume_values,
    quarters,
    tag
):
  
    echo = shutil.which("echo")
    sleep = shutil.which("sleep")
    python = shutil.which("python3")
    dotnet = shutil.which("dotnet")
    cp = shutil.which("cp")
    gzip = shutil.which("gzip")
    np.random.seed(seedKey)  # Set seed for reprodusability
    number_of_repeats = seedcount
    rnd_seeds = np.random.randint(0, high=100000, size=number_of_repeats)
    print(f"Generating simulations using the following seeds:{rnd_seeds}")
    days_to_simulate = quarters * 60
    #set variables from provided parameters
    n = 1
    n_files = 1
    configs = list()

    for market_fraction_value in market_fraction_values:
        for ammps_volume_value in ammps_volume_values:
            for seed in rnd_seeds:
                pkey = str(n)
                simDict = {
                    "PartitionKey": pkey,
                    "RowKey": experimentName
                    + str(n)
                    + "|"
                    + str(seed)
                    + "|"
                    + "Market_Fraction"
                    + "|"
                    + str(market_fraction_value)
                    + "ammps_volume"
                    + "|"
                    + str(ammps_volume_value),
                    "experimentName": experimentName,
                    "status": "pending",
                    "simid": n,
                    "market_fraction_value": market_fraction_value,
                    "quarters": quarters,
                    "tag": tag,
                    "ammps_config_cmd": "",
                    "start_ammps_cmd": "",
                    "start_sharkfin_cmd": "",
                    "seed": str(seed),
                    "sharkfin": {
                        "save_as": "/shared/home/ammpssharkfin/output/"
                        + experimentName
                        + str(n),
                        # "save_as" : "../../output/" + experimentName + str(n),
                        "parameters": {
                            "simulation": "Attention",
                            "tag": tag,
                            "seed": str(seed),
                            "quarters": quarters
                        },
                    },
                    "ammps": {
                        "ammps_config_file_name": "test_conf"
                        + str(n)
                        + ".xlsx",
                        "ammps_output_dir": "/mnt/sharkfin-ammps-fs/ammps_data_"
                        + experimentName
                        + str(n)
                        + "out",
                        "parameters": {
                            "instruments": 'ABC:NYSE:0.01:0.01,ABC:CITADEL:0.001:0.001',
                            "number": str(n),
                            "simulated-rpc": 'true',
                            "simulated-rpc-buy-sell-vol": ammps_volume_value,
                            "simulated-rpc-buy-sell-vol-std":1000
                        },
                    },
                    "ammps_config_gen": {
                        "parameters": {
                            "seed": str(seed),
                            "name": "test_conf" + str(n) + ".xlsx",
                            "days": quarters * 60,
                            "market_fraction": market_fraction_value,
                            "out-dir": "/usr/simulation/"
                        }
                    },
                }
                simDict["ammps_config_cmd"] = (
                    "/usr/bin/python /usr/simulation/ammps_config_generator/make_ggshark_config.py"
                    + dict_to_args(
                        simDict["ammps_config_gen"]["parameters"]
                    )
                )
                simDict["start_ammps_cmd"] = (
                    "dotnet /usr/simulation/ammps_bin/amm.engine.dll RunConfFromFile /usr/simulation/"
                    + simDict["ammps"]["ammps_config_file_name"]
                    + " "
                    + simDict["ammps"]["ammps_output_dir"]
                    + dict_to_args(simDict["ammps"]["parameters"])
                )
                simDict["start_sharkfin_cmd"] = (
                    "/usr/bin/python /usr/simulation/SHARKFin/simulate/run_any_simulation.py "
                    + simDict["sharkfin"]["save_as"]
                    + dict_to_args(simDict["sharkfin"]["parameters"])
                )
                simDict["sharkfin"]["parameters"] = json.dumps(
                    simDict["sharkfin"]["parameters"]
                )
                simDict["sharkfin"] = json.dumps(simDict["sharkfin"])
                simDict["ammps"]["parameters"] = json.dumps(
                    simDict["ammps"]["parameters"]
                )
                simDict["ammps"] = json.dumps(simDict["ammps"])
                simDict["ammps_config_gen"]["parameters"] = json.dumps(
                    simDict["ammps_config_gen"]["parameters"]
                )
                simDict["ammps_config_gen"] = json.dumps(
                    simDict["ammps_config_gen"]
                )
                simDict[
                    "cmdBundle"
                ] = f"{simDict['ammps_config_cmd']}{simDict['start_ammps_cmd']}{simDict['start_sharkfin_cmd']}"
                configs.append(simDict)
                n += 1
    return configs


    
    
    
records_row = 0
allclean_row = 0
def process_run(data, f, data_class):

    global records_row
    global allclean_row

    # auto magic operations to turn this into a json object
    idx = data.index('aLvl_mean') - 2
    json_data = data[:idx]
    idx = data.index('max_buy_limit') - 1
    json_data += data[idx:]
    json_data = json_data.replace(">", "")
    json_data = json_data.replace("<class", "")
    json_data = json_data.replace("'", "\"")
    json_data = json_data.replace("None", "0")
    json_data = json_data.replace("{\"bl\"", ":bl")
    json_data = json_data.replace("\"sl\"", "sl")
    json_data = json_data.replace("\"dividend\"", "dividend")
    json_data = json_data.replace("\"end_simulation\"", "end_simulation")
    json_data = json_data.replace("false}", "false")
    json_data = json_data.replace('nan',"-1")

    json_obj = json.loads(json_data)

    allclean_row += 1

    # add row to clean data file
    try:
        status_code=str(json_obj['status_code'])
    except Exception as e:
        print(e)

    # add row to clean data file
    try:
        all_clean_str = str(allclean_row) + "," + str(json_obj['max_buy_limit']) + "," + str(json_obj['max_sell_limit']) + \
            "," + str(json_obj['idx_max_buy_limit']) + "," + str(json_obj['idx_max_sell_limit']) + \
            "," + str(json_obj['mean_buy_limit']) + "," + str(json_obj['mean_sell_limit']) + \
            "," + str(json_obj['std_buy_limit']) + "," + str(json_obj['std_sell_limit']) + \
            "," + str(json_obj['kurtosis_buy_limit']) + "," + str(json_obj['kurtosis_sell_limit']) + \
            "," + str(json_obj['skew_buy_limit']) + "," + str(json_obj['skew_sell_limit']) + \
            "," + str(json_obj['min_asset_price']) + "," + str(json_obj['max_asset_price']) + \
            "," + str(json_obj['idx_min_asset_price']) + "," + str(json_obj['idx_max_asset_price']) + \
            "," + str(json_obj['std_asset_price']) + "," + str(json_obj['q']) + \
            "," + str(json_obj['r']) + "," + "[]" + \
            "," + str(json_obj['attention']) + "," + str(json_obj['ror_volatility']) + \
            "," + str(json_obj['ror_mean']) + "," + str(json_obj['total_population_aLvl_mean']) + \
            "," + str(json_obj['total_population_aLvl_std']) + "," + str(json_obj['dividend_growth_rate']) + \
            "," + str(json_obj['dividend_std']) + "," + str(json_obj['p1']) + \
            "," + str(json_obj['p2']) + "," + str(json_obj['delta_t1']) + \
            "," + str(json_obj['delta_t2']) + "," + str(json_obj['dollars_per_hark_money_unit']) + \
            "," + str(json_obj['seconds']) + "," + "\"" + str(json_obj['error_message']) + "\"" + \
            "," + str(json_obj['seed']) + "," + str(f) + "," + status_code
    except Exception as e:
        try:
            all_clean_str = str(allclean_row) + "," + str(json_obj['max_buy_limit']) + "," + str(json_obj['max_sell_limit']) + \
                "," + str(json_obj['idx_max_buy_limit']) + "," + str(json_obj['idx_max_sell_limit']) + \
                "," + str(json_obj['mean_buy_limit']) + "," + str(json_obj['mean_sell_limit']) + \
                "," + str(json_obj['std_buy_limit']) + "," + str(json_obj['std_sell_limit']) + \
                "," + str(-1) + "," + str(-1) + \
                "," + str(-1) + "," + str(-1) + \
                "," + str(json_obj['min_asset_price']) + "," + str(json_obj['max_asset_price']) + \
                "," + str(json_obj['idx_min_asset_price']) + "," + str(json_obj['idx_max_asset_price']) + \
                "," + str(json_obj['std_asset_price']) + "," + str(json_obj['q']) + \
                "," + str(json_obj['r']) + "," + "[]" + \
                "," + str(json_obj['attention']) + "," + str(json_obj['ror_volatility']) + \
                "," + str(json_obj['ror_mean']) + "," + str(json_obj['total_population_aLvl_mean']) + \
                "," + str(json_obj['total_population_aLvl_std']) + "," + str(json_obj['dividend_growth_rate']) + \
                "," + str(json_obj['dividend_std']) + "," + str(json_obj['p1']) + \
                "," + str(json_obj['p2']) + "," + str(json_obj['delta_t1']) + \
                "," + str(json_obj['delta_t2']) + "," + str(json_obj['dollars_per_hark_money_unit']) + \
                "," + str(json_obj['seconds']) + "," + "\"" + str(json_obj['error_message']) + "\"" + \
                "," + str(json_obj['seed']) + "," + str(f) + "," + status_code
        except Exception as f:
            print(f,json_obj)

    print("all_clean_str", all_clean_str)
    
    df = pd.read_csv(io.StringIO(data_class), sep=",")
    for ind in df.index:
        records_row += 1
        all_records_str = str(records_row) + "," + str(records_row) + "," + str(json_obj['attention']) + "," + str(json_obj['p1']) + "," + str(json_obj['p2']) + "," + \
            str(json_obj['dividend_growth_rate']) + "," + str(json_obj['ror_volatility']) + "," + str(json_obj['dollars_per_hark_money_unit']) + "," + \
            str(df['aLvl_mean'][ind]) + "," + str(df['CRRA'][ind]) + "," + str(df['DiscFac'][ind]) + "," + str(df['aLvl_std'][ind]) + "," + \
            str(df['mNrm_ratio_StE_mean'][ind]) + "," + str(df['mNrm_ratio_StE_std'][ind]) + "," + str(allclean_row) + "," + status_code
        
        print("all_records_str", all_records_str)
       
  



def run_cmd_remote(hostname, user, sshkeypath, command):
    # Create an SSH client
    ssh = SSHClient()

    # Automatically add the remote host key
    ssh.set_missing_host_key_policy(AutoAddPolicy())

    # Load the SSH private key
    private_key = RSAKey.from_private_key_file(sshkeypath)

    # Connect to the remote machine
    ssh.connect(hostname=hostname, username=user, pkey=private_key)

    # command to execute on client
    #scmd = f'sudo -i ls {remotepath}'
    scmd = command
    print(scmd)
    stdin, stdout, stderr = ssh.exec_command(scmd)

    # Read the output and error streams
    output = stdout.read().decode()
    error = stderr.read().decode()

    # Close the input, output, and error streams
    stdin.close()
    stdout.close()
    stderr.close()
    # Return the output and error
    return output, error

    # Close the SSH connection
    ssh.close()
    
    
    
def get_simulation_result(experimentName,simid):
    simcmd = f"tail ./output/logs/slurm/{experimentName}{simid}_sim_stats.txt"
    classcmd = f"tail ./output/logs/slurm/{experimentName}{simid}._class_stats.csv"
    scmd = f"ssh -i {key} {user}@{host} {simcmd}"
    ccmd = f"ssh -i {key} {user}@{host} {classcmd}"
    c1out, c1err = subprocess.Popen(
        scmd,
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        encoding="utf-8",
    ).communicate()
    c2out, c2err = subprocess.Popen(
        ccmd,
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        encoding="utf-8",
    ).communicate()
    # self.simstats = c1out
    # self.classtats = c2out
    simstats = c1out
    classtats = c2out
    return simstats, classtats

import json
import datetime
import os
import jwt
import seaborn as sns
import uuid
import time
import yaml
import pandas as pd
import numpy as np
import math
import matplotlib.pyplot as plt
import subprocess
import shlex
import shutil
import argparse
import io
import paramiko
import zipfile
from paramiko import SSHClient
from paramiko import RSAKey
from paramiko import AutoAddPolicy
from scp import SCPClient
from pprint import pprint as pp
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from azure.keyvault.keys import KeyClient
from azure.keyvault.secrets import SecretClient
from azure.identity import DefaultAzureCredential
from azure.identity import InteractiveBrowserCredential
from azure.data.tables import TableServiceClient
from azure.data.tables import TableClient
from azure.data.tables import UpdateMode
from base64 import urlsafe_b64encode
from cryptography.hazmat.primitives import serialization
import matplotlib.pyplot as plt
import pandas as pd


def build_lucas0_configs(
    experimentName,
    seedcount,
    seedKey,
    mmLucasFactors,
    instValStds,
    quarters,
    tag
):
  
    echo = shutil.which("echo")
    sleep = shutil.which("sleep")
    python = shutil.which("python3")
    dotnet = shutil.which("dotnet")
    cp = shutil.which("cp")
    gzip = shutil.which("gzip")
    np.random.seed(seedKey)  # Set seed for reprodusability
    number_of_repeats = seedcount
    rnd_seeds = np.random.randint(0, high=100000, size=number_of_repeats)
    print(f"Generating simulations using the following seeds:{rnd_seeds}")
    days_to_simulate = quarters * 60
    #set variables from provided parameters
    n = 1
    n_files = 1
    configs = list()

    for mmLucasFactor in mmLucasFactors:
        for instValStd in instValStds:
            for seed in rnd_seeds:
                pkey = str(n)
                simDict = {
                    "PartitionKey": pkey,
                    "RowKey": experimentName
                    + str(n)
                    + "|"
                    + str(seed)
                    + "|"
                    + "mmLucasFactor"
                    + "|"
                    + str(mmLucasFactor)
                    + "instValStd"
                    + "|"
                    + str(instValStd),
                    "experimentName": experimentName,
                    "status": "pending",
                    "simid": n,
                    "mmlucasfactor": mmLucasFactor,
                    "inst_val_std":instValStd,
                    "quarters": quarters,
                    "tag": tag,
                    "ammps_config_cmd": "",
                    "start_ammps_cmd": "",
                    "start_sharkfin_cmd": "",
                    "seed": str(seed),
                    "sharkfin": {
                        "save_as": "/shared/home/ammpssharkfin/output/"
                        + experimentName
                        + str(n),
                        # "save_as" : "../../output/" + experimentName + str(n),
                        "parameters": {
                            "simulation": "Attention",
                            "tag": tag,
                            "seed": str(seed),
                            "quarters": quarters
                        },
                    },
                    "ammps": {
                        "ammps_config_file_name": "test_conf"
                        + str(n)
                        + ".xlsx",
                        "ammps_output_dir": "/shared/home/ammpssharkfin/output/"
                        + experimentName
                        + str(n)
                        + "out",
                        "parameters": {
                            "number": str(n),
                            "compression": 'true',
                            "simulated-rpc": 'true',
                            "simulated-rpc-buy-sell-vol": 0,
                            "simulated-rpc-buy-sell-vol-std":0,
                            "prefix":'norpc'
                        },
                    },
                    "ammps_config_gen": {
                        "parameters": {
                            "seed": str(seed),
                            "name": "test_conf" + str(n) + ".xlsx",
                            "days": quarters * 60,
                            "mm_lucas_factor": mmLucasFactor,
                            "inst_val_std": instValStd,
                            "out-dir": "/usr/simulation/"
                        }
                    },
                }
                simDict["ammps_config_cmd"] = (
                    "/usr/bin/python3 /usr/simulation/ammps_config_generator/acg/simulations/make_lucas_shark_config.py"
                    + dict_to_args(
                        simDict["ammps_config_gen"]["parameters"]
                    )
                )
                simDict["start_ammps_cmd"] = (
                    "dotnet /usr/simulation/ammps_bin/amm.engine.dll RunConfFromFile /usr/simulation/"
                    + simDict["ammps"]["ammps_config_file_name"]
                    + " "
                    + simDict["ammps"]["ammps_output_dir"]
                    + dict_to_args(simDict["ammps"]["parameters"])
                )
                simDict["start_sharkfin_cmd"] = (
                    "/usr/bin/python3 /usr/simulation/SHARKFin/simulate/run_any_simulation.py "
                    + simDict["sharkfin"]["save_as"]
                    + dict_to_args(simDict["sharkfin"]["parameters"])
                )
                simDict["sharkfin"]["parameters"] = json.dumps(
                    simDict["sharkfin"]["parameters"]
                )
                simDict["sharkfin"] = json.dumps(simDict["sharkfin"])
                simDict["ammps"]["parameters"] = json.dumps(
                    simDict["ammps"]["parameters"]
                )
                simDict["ammps"] = json.dumps(simDict["ammps"])
                simDict["ammps_config_gen"]["parameters"] = json.dumps(
                    simDict["ammps_config_gen"]["parameters"]
                )
                simDict["ammps_config_gen"] = json.dumps(
                    simDict["ammps_config_gen"]
                )
                simDict["cmdBundle"] = f"{simDict['ammps_config_cmd']}{simDict['start_ammps_cmd']}{simDict['start_sharkfin_cmd']}"
                configs.append(simDict)
                n += 1
    return configs


def build_lucasShark_configs(
    experimentName,
    seedcount,
    seedKey,
    rpc_host,
    mmLucasFactors,
    instValStds,
    attention_values,
    dphms,
    zetas,
    quarters,
    tag
):
  
    echo = shutil.which("echo")
    sleep = shutil.which("sleep")
    python = shutil.which("python3")
    dotnet = shutil.which("dotnet")
    cp = shutil.which("cp")
    gzip = shutil.which("gzip")
    np.random.seed(seedKey)  # Set seed for reprodusability
    number_of_repeats = seedcount
    rnd_seeds = np.random.randint(0, high=100000, size=number_of_repeats)
    print(f"Generating simulations using the following seeds:{rnd_seeds}")
    days_to_simulate = quarters * 60
    #set variables from provided parameters
    n = 1
    n_files = 1
    configs = list()

    for attention_value in attention_values:
        for dphm in dphms:
            for zeta in zetas:
                for mmLucasFactor in mmLucasFactors:
                    for instValStd in instValStds:
                        for seed in rnd_seeds:
                            pkey = str(n)
                            simDict = {
                                "PartitionKey": pkey,
                                "RowKey": experimentName
                                + str(n)
                                + "|"
                                + str(seed)
                                + "|"
                                + "mmLucasFactor"
                                + "|"
                                + str(mmLucasFactor)
                                + "instValStd"
                                + "|"
                                + str(instValStd),
                                "experimentName": experimentName,
                                "status": "pending",
                                "simid": n,
                                "mmlucasfactor": mmLucasFactor,
                                "inst_val_std":instValStd,
                                "quarters": quarters,
                                "attention": attention_value,
                                "simulation":"Attention",
                                "expectations":"InferentialExpectations",
                                "zeta":zeta,
                                "dphm":dphm,
                                "tag": tag,
                                "ammps_config_cmd": "",
                                "start_ammps_cmd": "",
                                "start_sharkfin_cmd": "",
                                "seed": str(seed),
                                "sharkfin": {
                                    "save_as": "/shared/home/ammpssharkfin/output/"
                                    + experimentName
                                    + str(n),
                                    # "save_as" : "../../output/" + experimentName + str(n),
                                    "parameters": {
                                        "simulation": "Attention",
                                        "expectations": "InferentialExpectations",
                                        "market": "ClientRPCMarket",
                                        "zeta":zeta,
                                        "attention":attention_value,
                                        "dphm":dphm,
                                        "queue": experimentName + str(n),
                                        "rhost": rpc_host,
                                        "tag": tag,
                                        "seed": str(seed),
                                        "quarters": quarters
                                    },
                                },
                                "ammps": {
                                    "ammps_config_file_name": "test_conf"
                                    + str(n)
                                    + ".xlsx",
                                    "ammps_output_dir": "/shared/home/ammpssharkfin/output/"
                                    + experimentName
                                    + str(n)
                                    + "out",
                                    "parameters": {
                                        "number": str(n),
                                        "compression": 'true',
                                        "rabbitMQ-host": rpc_host,
                                        "rabbitMQ-queue": experimentName + str(n),
                                        #"simulated-rpc": 'false',
                                        #"simulated-rpc-buy-sell-vol": 0,
                                        #"simulated-rpc-buy-sell-vol-std":0,
                                        "prefix":'lshark'
                                    },
                                },
                                "ammps_config_gen": {
                                    "parameters": {
                                        "seed": str(seed),
                                        "name": "test_conf" + str(n) + ".xlsx",
                                        "days": quarters * 60,
                                        "mm_lucas_factor": mmLucasFactor,
                                        "inst_val_std": instValStd,
                                        "out-dir": "/usr/simulation/"
                                    }
                                },
                            }
                            simDict["ammps_config_cmd"] = (
                                "/usr/bin/python3 /usr/simulation/ammps_config_generator/acg/simulations/make_lucas_shark_config.py"
                                + dict_to_args(
                                    simDict["ammps_config_gen"]["parameters"]
                                )
                            )
                            simDict["start_ammps_cmd"] = (
                                "dotnet /usr/simulation/ammps_bin/amm.engine.dll RunConfFromFile /usr/simulation/"
                                + simDict["ammps"]["ammps_config_file_name"]
                                + " "
                                + simDict["ammps"]["ammps_output_dir"]
                                + dict_to_args(simDict["ammps"]["parameters"])
                            )
                            simDict["start_sharkfin_cmd"] = (
                                "/usr/bin/python3 /usr/simulation/SHARKFin/simulate/run_any_simulation.py "
                                + simDict["sharkfin"]["save_as"]
                                + dict_to_args(simDict["sharkfin"]["parameters"])
                            )
                            simDict["sharkfin"]["parameters"] = json.dumps(
                                simDict["sharkfin"]["parameters"]
                            )
                            simDict["sharkfin"] = json.dumps(simDict["sharkfin"])
                            simDict["ammps"]["parameters"] = json.dumps(
                                simDict["ammps"]["parameters"]
                            )
                            simDict["ammps"] = json.dumps(simDict["ammps"])
                            simDict["ammps_config_gen"]["parameters"] = json.dumps(
                                simDict["ammps_config_gen"]["parameters"]
                            )
                            simDict["ammps_config_gen"] = json.dumps(
                                simDict["ammps_config_gen"]
                            )
                            simDict[
                                "cmdBundle"
                            ] = f"{simDict['ammps_config_cmd']}{simDict['start_ammps_cmd']}{simDict['start_sharkfin_cmd']}"
                            configs.append(simDict)
                            n += 1
    return configs

def build_sparkShark_configs(
    experimentName,
    seedcount,
    seedKey,
    rpc_host,
    mmLucasFactors,
    instValStds,
    attention_values,
    dphms,
    zetas,
    pop_aNrmInitMeans,
    quarters,
    tag
):
  
    echo = shutil.which("echo")
    sleep = shutil.which("sleep")
    python = shutil.which("python3")
    dotnet = shutil.which("dotnet")
    cp = shutil.which("cp")
    gzip = shutil.which("gzip")
    np.random.seed(seedKey)  # Set seed for reprodusability
    number_of_repeats = seedcount
    rnd_seeds = np.random.randint(0, high=100000, size=number_of_repeats)
    print(f"Generating simulations using the following seeds:{rnd_seeds}")
    days_to_simulate = quarters * 60
    #set variables from provided parameters
    n = 1
    n_files = 1
    configs = list()

    for attention_value in attention_values:
        for dphm in dphms:
            for zeta in zetas:
                for mmLucasFactor in mmLucasFactors:
                    for instValStd in instValStds:
                        for pop_aNrmInitMean in pop_aNrmInitMeans:
                            for seed in rnd_seeds:
                                pkey = str(n)
                                simDict = {
                                    "PartitionKey": pkey,
                                    "RowKey": experimentName
                                    + str(n)
                                    + "|"
                                    + str(seed)
                                    + "|"
                                    + "mmLucasFactor"
                                    + "|"
                                    + str(mmLucasFactor)
                                    + "instValStd"
                                    + "|"
                                    + str(instValStd),
                                    "experimentName": experimentName,
                                    "status": "pending",
                                    "simid": n,
                                    "mmlucasfactor": mmLucasFactor,
                                    "inst_val_std":instValStd,
                                    "quarters": quarters,
                                    "attention": attention_value,
                                    "simulation":"Attention",
                                    "expectations":"InferentialExpectations",
                                    "zeta":zeta,
                                    "dphm":dphm,
                                    "pop_aNrmInitMean":pop_aNrmInitMean,
                                    "tag": tag,
                                    "ammps_config_cmd": "",
                                    "start_ammps_cmd": "",
                                    "start_sharkfin_cmd": "",
                                    "seed": str(seed),
                                    "sharkfin": {
                                        "save_as": "/shared/home/ammpssharkfin/output/"
                                        + experimentName
                                        + str(n),
                                        # "save_as" : "../../output/" + experimentName + str(n),
                                        "parameters": {
                                            "simulation": "Attention",
                                            "expectations": "InferentialExpectations",
                                            "market": "ClientRPCMarket",
                                            "zeta":zeta,
                                            "pop_aNrmInitMean":pop_aNrmInitMean,
                                            "attention":attention_value,
                                            "dphm":dphm,
                                            "queue": experimentName + str(n),
                                            "rhost": rpc_host,
                                            "tag": tag,
                                            "seed": str(seed),
                                            "quarters": quarters
                                        },
                                    },
                                    "ammps": {
                                        "ammps_config_file_name": "test_conf"
                                        + str(n)
                                        + ".xlsx",
                                        "ammps_output_dir": "/shared/home/ammpssharkfin/output/"
                                        + experimentName
                                        + str(n)
                                        + "out",
                                        "parameters": {
                                            "number": str(n),
                                            "compression": 'true',
                                            "rabbitMQ-host": rpc_host,
                                            "rabbitMQ-queue": experimentName + str(n),
                                            #"simulated-rpc": 'false',
                                            #"simulated-rpc-buy-sell-vol": 0,
                                            #"simulated-rpc-buy-sell-vol-std":0,
                                            "prefix":'lshark'
                                        },
                                    },
                                    "ammps_config_gen": {
                                        "parameters": {
                                            "seed": str(seed),
                                            "name": "test_conf" + str(n) + ".xlsx",
                                            "days": quarters * 60,
                                            "mm_lucas_factor": mmLucasFactor,
                                            "inst_val_std": instValStd,
                                            "out-dir": "/usr/simulation/"
                                        }
                                    },
                                }
                                simDict["ammps_config_cmd"] = (
                                    "/usr/bin/python3 /usr/simulation/ammps_config_generator/acg/simulations/make_lucas_shark_config.py"
                                    + dict_to_args(
                                        simDict["ammps_config_gen"]["parameters"]
                                    )
                                )
                                simDict["start_ammps_cmd"] = (
                                    "dotnet /usr/simulation/ammps_bin/amm.engine.dll RunConfFromFile /usr/simulation/"
                                    + simDict["ammps"]["ammps_config_file_name"]
                                    + " "
                                    + simDict["ammps"]["ammps_output_dir"]
                                    + dict_to_args(simDict["ammps"]["parameters"])
                                )
                                simDict["start_sharkfin_cmd"] = (
                                    "/usr/bin/python3 /usr/simulation/SHARKFin/simulate/run_any_simulation.py "
                                    + simDict["sharkfin"]["save_as"]
                                    + dict_to_args(simDict["sharkfin"]["parameters"])
                                )
                                simDict["sharkfin"]["parameters"] = json.dumps(
                                    simDict["sharkfin"]["parameters"]
                                )
                                simDict["sharkfin"] = json.dumps(simDict["sharkfin"])
                                simDict["ammps"]["parameters"] = json.dumps(
                                    simDict["ammps"]["parameters"]
                                )
                                simDict["ammps"] = json.dumps(simDict["ammps"])
                                simDict["ammps_config_gen"]["parameters"] = json.dumps(
                                    simDict["ammps_config_gen"]["parameters"]
                                )
                                simDict["ammps_config_gen"] = json.dumps(
                                    simDict["ammps_config_gen"]
                                )
                                simDict[
                                    "cmdBundle"
                                ] = f"{simDict['ammps_config_cmd']}{simDict['start_ammps_cmd']}{simDict['start_sharkfin_cmd']}"
                                configs.append(simDict)
                                n += 1
    return configs

def build_sparkShark_configs(
    experimentName,
    seedcount,
    seedKey,
    rpc_host,
    mmLucasFactors,
    instValStds,
    attention_values,
    dphms,
    zetas,
    pop_aNrmInitMeans,
    quarters,
    tag
):
  
    echo = shutil.which("echo")
    sleep = shutil.which("sleep")
    python = shutil.which("python3")
    dotnet = shutil.which("dotnet")
    cp = shutil.which("cp")
    gzip = shutil.which("gzip")
    np.random.seed(seedKey)  # Set seed for reprodusability
    number_of_repeats = seedcount
    rnd_seeds = np.random.randint(0, high=100000, size=number_of_repeats)
    print(f"Generating simulations using the following seeds:{rnd_seeds}")
    days_to_simulate = quarters * 60
    #set variables from provided parameters
    n = 1
    n_files = 1
    configs = list()

    for attention_value in attention_values:
        for dphm in dphms:
            for zeta in zetas:
                for mmLucasFactor in mmLucasFactors:
                    for instValStd in instValStds:
                        for pop_aNrmInitMean in pop_aNrmInitMeans:
                            for seed in rnd_seeds:
                                pkey = str(n)
                                simDict = {
                                    "PartitionKey": pkey,
                                    "RowKey": experimentName
                                    + str(n)
                                    + "|"
                                    + str(seed)
                                    + "|"
                                    + "mmLucasFactor"
                                    + "|"
                                    + str(mmLucasFactor)
                                    + "instValStd"
                                    + "|"
                                    + str(instValStd),
                                    "experimentName": experimentName,
                                    "status": "pending",
                                    "simid": n,
                                    "mmlucasfactor": mmLucasFactor,
                                    "inst_val_std":instValStd,
                                    "quarters": quarters,
                                    "attention": attention_value,
                                    "simulation":"Attention",
                                    "expectations":"InferentialExpectations",
                                    "zeta":zeta,
                                    "dphm":dphm,
                                    "pop_aNrmInitMean":pop_aNrmInitMean,
                                    "tag": tag,
                                    "ammps_config_cmd": "",
                                    "start_ammps_cmd": "",
                                    "start_sharkfin_cmd": "",
                                    "seed": str(seed),
                                    "sharkfin": {
                                        "save_as": "/shared/home/ammpssharkfin/output/"
                                        + experimentName
                                        + str(n),
                                        # "save_as" : "../../output/" + experimentName + str(n),
                                        "parameters": {
                                            "simulation": "Attention",
                                            "expectations": "InferentialExpectations",
                                            "market": "ClientRPCMarket",
                                            "zeta":zeta,
                                            "pop_aNrmInitMean":pop_aNrmInitMean,
                                            "attention":attention_value,
                                            "dphm":dphm,
                                            "queue": experimentName + str(n),
                                            "rhost": rpc_host,
                                            "tag": tag,
                                            "seed": str(seed),
                                            "quarters": quarters
                                        },
                                    },
                                    "ammps": {
                                        "ammps_config_file_name": "test_conf"
                                        + str(n)
                                        + ".xlsx",
                                        "ammps_output_dir": "/shared/home/ammpssharkfin/output/"
                                        + experimentName
                                        + str(n)
                                        + "out",
                                        "parameters": {
                                            "number": str(n),
                                            "compression": 'true',
                                            "rabbitMQ-host": rpc_host,
                                            "rabbitMQ-queue": experimentName + str(n),
                                            #"simulated-rpc": 'false',
                                            #"simulated-rpc-buy-sell-vol": 0,
                                            #"simulated-rpc-buy-sell-vol-std":0,
                                            "prefix":'lshark'
                                        },
                                    },
                                    "ammps_config_gen": {
                                        "parameters": {
                                            "seed": str(seed),
                                            "name": "test_conf" + str(n) + ".xlsx",
                                            "days": quarters * 60,
                                            "mm_lucas_factor": mmLucasFactor,
                                            "inst_val_std": instValStd,
                                            "out-dir": "/usr/simulation/"
                                        }
                                    },
                                }
                                simDict["ammps_config_cmd"] = (
                                    "/usr/bin/python3 /usr/simulation/ammps_config_generator/acg/simulations/make_lucas_shark_config.py"
                                    + dict_to_args(
                                        simDict["ammps_config_gen"]["parameters"]
                                    )
                                )
                                simDict["start_ammps_cmd"] = (
                                    "dotnet /usr/simulation/ammps_bin/amm.engine.dll RunConfFromFile /usr/simulation/"
                                    + simDict["ammps"]["ammps_config_file_name"]
                                    + " "
                                    + simDict["ammps"]["ammps_output_dir"]
                                    + dict_to_args(simDict["ammps"]["parameters"])
                                )
                                simDict["start_sharkfin_cmd"] = (
                                    "/usr/bin/python3 /usr/simulation/SHARKFin/simulate/run_any_simulation.py "
                                    + simDict["sharkfin"]["save_as"]
                                    + dict_to_args(simDict["sharkfin"]["parameters"])
                                )
                                simDict["sharkfin"]["parameters"] = json.dumps(
                                    simDict["sharkfin"]["parameters"]
                                )
                                simDict["sharkfin"] = json.dumps(simDict["sharkfin"])
                                simDict["ammps"]["parameters"] = json.dumps(
                                    simDict["ammps"]["parameters"]
                                )
                                simDict["ammps"] = json.dumps(simDict["ammps"])
                                simDict["ammps_config_gen"]["parameters"] = json.dumps(
                                    simDict["ammps_config_gen"]["parameters"]
                                )
                                simDict["ammps_config_gen"] = json.dumps(
                                    simDict["ammps_config_gen"]
                                )
                                simDict[
                                    "cmdBundle"
                                ] = f"{simDict['ammps_config_cmd']}{simDict['start_ammps_cmd']}{simDict['start_sharkfin_cmd']}"
                                configs.append(simDict)
                                n += 1
    return configs



def convert_values_to_strings(dictionary):
    for key in dictionary:
        dictionary[key] = str(dictionary[key])
    return dictionary


def load_simStats_file_from_container(container, blob_name):
    connect_str = "DefaultEndpointsProtocol=https;AccountName=sbsimulationdata;AccountKey=KG8uTvfQinDCQoJYycZ+PvB+jw5/ovAp7ZfPaMLaCU53wKtg4QThAJ2IowOqd60+tr32kLD96lkt+AStExWHNQ==;EndpointSuffix=core.windows.net"
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)
    container_client = blob_service_client.get_container_client(container)
    blob_client = container_client.get_blob_client(blob=blob_name)

    # Download blob data
    data = blob_client.download_blob().readall()

    # Save blob data to a temporary file
    temp_file_path = "/tmp/{}".format(blob_name)  # Update the path as needed
    with open(temp_file_path, "wb") as file:
        file.write(data)
   
    # Load data into pandas DataFrame
    data = json.loads(data)
    #data = convert_values_to_strings(data)
    index = ['0']
    df = pd.DataFrame(data, index)  # Use the appropriate read function for your file format
    
    # Cleanup: Delete the temporary file
    os.remove(temp_file_path)
    return df

def load_classStats_file_from_container(container, blob_name):
    connect_str = "DefaultEndpointsProtocol=https;AccountName=sbsimulationdata;AccountKey=KG8uTvfQinDCQoJYycZ+PvB+jw5/ovAp7ZfPaMLaCU53wKtg4QThAJ2IowOqd60+tr32kLD96lkt+AStExWHNQ==;EndpointSuffix=core.windows.net"
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)
    container_client = blob_service_client.get_container_client(container)
    blob_client = container_client.get_blob_client(blob=blob_name)

    # Download blob data
    data = blob_client.download_blob().readall()

    # Save blob data to a temporary file
    temp_file_path = "/tmp/{}".format(blob_name)  # Update the path as needed
    with open(temp_file_path, "wb") as file:
        file.write(data)

    # Load data into pandas DataFrame
    df = pd.read_csv(temp_file_path)  # Use the appropriate read function for your file format

    # Print the DataFrame or perform further operations
    print(df)

    # Cleanup: Delete the temporary file
    os.remove(temp_file_path)
    return df
# Example usage
#df = load_stat_file_from_container("lucashark3", "whiteShark10reps1000-whiteShark10reps_data.csv")

def getSlurmJobLogs(schedulerIP,experimentName, jobID, simID, clusterKey, user):
    logDir = '/output/logs/slurm/'
    slurmOut = f"{logDir}{experimentName}job_{jobID}_{simID}.out.log"
    slurmErr = f"{logDir}{experimentName}job_{jobID}_{simID}.out.log"
    
    scmd= f"cat {slurmOut}"
    runner = f"ssh -o StrictHostKeyChecking=no -i {clusterKey} {user}@{schedulerIP} {scmd}"
    out, err = subprocess.Popen(scmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True).communicate()
    if out:
        print(f"Attempting to connect to {schedulerIP}.......")
        print("Connected")
        print(out)
    if err:
        print(err)
        
        
            
def experiment_cost(num_sims, cores_per_sim, sim_duration, cost_per_core):
    total_cores = num_sims * cores_per_sim
    sim_hours = sim_duration / 3600  # Convert duration to hours
    total_cost = total_cores * sim_hours * cost_per_core
    return total_cost
