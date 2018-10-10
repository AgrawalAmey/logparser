#!/usr/bin/env python

import sys
sys.path.append('../')
from logparser import MoLFI, evaluator
import os
import pandas as pd


configs = [
    {
        'path': '../../datasets/',
        'logName': 'BGL.log',
        'savePath': '../../results/MoLFI/BGL/',
        'rex': ['core\.[0-9]*'],
        'log_format': '<Label> <Timestamp> <Date> <Node> <Time> <NodeRepeat> <Type> <Component> <Level> <Content>',
    },
    {
        'path': '../../datasets/',
        'logName': 'HPC.log',
        'savePath': '../../results/MoLFI/HPC/',
        'rex': ['([0-9]+\.){3}[0-9]', 'node-[0-9]+'],
        'log_format': '<LogId> <Node> <Component> <State> <Time> <Flag> <Content>',
    },
    {
        'path': '../../datasets/',
        'logName': 'HDFS.log',
        'log_format': '<Date> <Time> <Pid> <Level> <Component>: <Content>',
        'savePath': '../../results/MoLFI/HDFS/',
        'rex': ['blk_(|-)[0-9]+', '(/|)([0-9]+\.){3}[0-9]+(:[0-9]+|)(:|)'],
    },
    {
        'path': '../../datasets/',
        'logName': 'Zookeeper.log',
        'savePath': '../../results/MoLFI/Zookeeper/',
        'rex': ['(/|)([0-9]+\.){3}[0-9]+(:[0-9]+|)(:|)'],
        'log_format': '<Date> <Time> - <Level>  \[<Node>:<Component>@<Id>\] - <Content>',
    },
    {
        'path': '../../datasets/',
        'logName': 'Linux.log',
        'savePath': '../../results/MoLFI/Linux/',
        'rex': ['([0-9]+\.){3}[0-9]+'],
        'log_format': '<Month> <Date> <Time> <Level> <Component>(\[<PID>\])?: <Content>',
    },
    {
        'path': '../../datasets/',
        'logName': 'Apache.log',
        'savePath': '../../results/MoLFI/Apache/',
        'rex': ['(\d+\.){3}\d+'],
        'log_format': '\[<Time>\] \[<Level>\] <Content>',
    },
    {
        'path': '../../datasets/',
        'logName': 'Proxifier.log',
        'savePath': '../../results/MoLFI/Proxifier/',
        'rex': ['<\d+\ssec', '([\w-]+\.)+[\w-]+(:\d+)?', '\d{2}:\d{2}(:\d{2})*', '\b[KGTM]?B\b'],
        'log_format': '\[<Time>\] <Program> - <Content>',
    }
    # # {
    # # 	'path': '../../qdatastes'
    # '# 	logName': 'Spark.log',
    # #     log_format: '<Date> <Time> <Level> <Component>: <Content>',
    # '#' 	'savePath': '../../results/MoLFI/Spark/
    # '#' 	rex: [],
    # # },
    # # 		{
    # # 	'path': '../../qdatastes'
    # '# 	logName': 'Hive.log',
    # # 	'savePath': '../../results/MoLFI/Hive/
    # '#' 	rex: [],
    # '#' },
    # # {
    # # 	'path': '../../qdatastes'
    # '# 	logName': 'Presto.log',
    # # 	'savePath': '../../results/MoLFI/Presto/
    # '#' 	rex: [],
    # '#' }
]

for config in configs:
    parser = MoLFI.LogParser(log_format=config["log_format"], indir=config["path"],
                             outdir=config["savePath"], rex=config['rex'], n_workers=32)
    parser.parse(config["logName"])