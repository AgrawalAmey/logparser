#!/usr/bin/env python

import sys
sys.path.append('../')
from logparser import SHISO, evaluator
import os
import pandas as pd


configs = [
    {
        'path': '../../datasets/',
        'logName': 'BGL.log',
        'savePath': '../../results/SHISO/BGL/',
        'rex': [('core\.[0-9]*', 'coreNum')],
        'log_format': '<Label> <Timestamp> <Date> <Node> <Time> <NodeRepeat> <Type> <Component> <Level> <Content>',
        'maxChildNum': 4,
        'mergeThreshold': 0.1,
        'formatLookupThreshold': 0.3,
        'superFormatThreshold': 0.85
    },
    {
        'path': '../../datasets/',
        'logName': 'HPC.log',
        'savePath': '../../results/SHISO/HPC/',
        'rex': [('([0-9]+\.){3}[0-9]', 'IPAdd'), ('node-[0-9]+', 'nodeNum')],
        'log_format': '<LogId> <Node> <Component> <State> <Time> <Flag> <Content>',
        'maxChildNum': 4,
        'mergeThreshold': 0.1,
        'formatLookupThreshold': 0.3,
        'superFormatThreshold': 0.85
    },
    {
        'path': '../../datasets/',
        'logName': 'HDFS.log',
        'log_format': '<Date> <Time> <Pid> <Level> <Component'>': <Content>',
        'savePath': '../../results/SHISO/HDFS/',
        'rex': [('blk_(|-)[0-9]+', 'blkID'), ('(/|)([0-9]+\.){3}[0-9]+(:[0-9]+|)(:|)', 'IPAddandPortID')],
        'maxChildNum': 4,
        'mergeThreshold': 0.1,
        'formatLookupThreshold': 0.3,
        'superFormatThreshold': 0.85
    },
    {
        'path': '../../datasets/',
        'logName': 'Zookeeper.log',
        'savePath': '../../results/SHISO/Zookeeper/',
        'rex': [('(/|)([0-9]+\.){3}[0-9]+(:[0-9]+|)(:|)', 'IPAddandPortID')],
        'log_format': '<Date> <Time> - <Level>  \[<Node>:<Component>@<Id>\] - <Content>',
        'maxChildNum': 4,
        'mergeThreshold': 0.1,
        'formatLookupThreshold': 0.3,
        'superFormatThreshold': 0.85
    },
    {
        'path': '../../datasets/',
        'logName': 'Linux.log',
        'savePath': '../../results/SHISO/Linux/',
        'rex': [('([0-9]+\.){3}[0-9]+', 'IPAdd')],
        'log_format': '<Month> <Date> <Time> <Level> <Component>(\[<PID'>\])?': <Content>',
        'maxChildNum': 4,
        'mergeThreshold': 0.1,
        'formatLookupThreshold': 0.3,
        'superFormatThreshold': 0.85
    },
    {
        'path': '../../datasets/',
        'logName': 'Apache.log',
        'savePath': '../../results/SHISO/Apache/',
        'rex': [('(\d+\.){3}\d+', '')],
        'log_format': '\[<Time>\] \[<Level>\] <Content>',
        'maxChildNum': 4,
        'mergeThreshold': 0.1,
        'formatLookupThreshold': 0.3,
        'superFormatThreshold': 0.85
    },
    {
        'path': '../../datasets/',
        'logName': 'Proxifier.log',
        'savePath': '../../results/SHISO/Proxifier/',
        'rex': [('<\d+\ssec', ''), ('([\w-]+\.)+[\w-]+(:\d+)?', ''), ('\d{2}:\d{2}(:\d{2})*', ''), ('\b[KGTM]?B\b', '')],
        'log_format': '\[<Time>\] <Program> - <Content>',
        'maxChildNum': 4,
        'mergeThreshold': 0.002,
        'formatLookupThreshold': 0.3,
        'superFormatThreshold': 0.85
    },
    # {
    # 	'path': '../../qdatastes'
    # 	'logName': 'Spark.log',
    #     'log_format': '<Date> <Time> <Level> <Component'>': <Content>',
    # 	'savePath': '../../results/SHISO/Spark/,
    # 	'rex': [],
    # },
    # 		{
    # 	'path': '../../qdatastes'
    # 	'logName': 'Hive.log',
    # 	'savePath': '../../results/SHISO/Hive/,
    # 	'rex': [],
    # },
    # {
    # 	'path': '../../qdatastes'
    # 	'logName': 'Presto.log',
    # 	'savePath': '../../results/SHISO/Presto/,
    # 	'rex': [],
    # }
]

for config in configs:
    parser = MoLFI.LogParser(log_format=config["log_format"], indir=config["path"],
                             outdir=config["savePath"], rex=setting['rex'],
                             maxChildNum=config["maxChildNum"], mergeThreshold=config["mergeThreshold"],
                             formatLookupThreshold=config["formatLookupThreshold"],
                             superFormatThreshold=config["superFormatThreshold"])
    parser.parse(config["logName"])