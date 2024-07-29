import shutil
import numpy as np
import argparse
import json
import datetime
import os

def dict_to_args(input_dict):
    string = " "
    for k in input_dict:
        v = input_dict[k]
        string += "--{} {} ".format(str(k), str(v))
    return string

def build_mmsparkShark_configs(
    experimentName,
    seedcount,
    seedKey,
    rpc_host,
    mmLucasFactors,
    mmsizes,
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
                    for mmsize in mmsizes:
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
                                        "mmsize":mmsize,
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
                                                "mm_size":mmsize,
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
