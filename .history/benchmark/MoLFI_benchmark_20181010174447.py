#!/usr/bin/env python

import sys
sys.path.append('../')
from logparser import MoLFI, evaluator
import os
import pandas as pd


input_dir = '../../datasets/' # The input directory of log file
output_dir = 'MoLFI_result/' # The output directory of parsing results


configs = [
    {
        path: '../../datasets/'
        logName: 'BGL.log',
        savePath: '../../results/BGL/'
        rex: [('core\.[0-9]*', 'coreNum')],
        log_format: '<Label> <Timestamp> <Date> <Node> <Time> <NodeRepeat> <Type> <Component> <Level> <Content>',
    },
    {
        path: '../../datasets/'
        logName: 'HPC.log',
        savePath: '../../results/HPC/'
        rex: [('([0-9]+\.){3}[0-9]', 'IPAdd'), ('node-[0-9]+', 'nodeNum')],
        log_format: '<LogId> <Node> <Component> <State> <Time> <Flag> <Content>',
    },
    {
        path: '../../datasets/'
        logName: 'HDFS.log',
        log_format: '<Date> <Time> <Pid> <Level> <Component>: <Content>',
        savePath: '../../results/HDFS/'
        rex: [('blk_(|-)[0-9]+', 'blkID'), ('(/|)([0-9]+\.){3}[0-9]+(:[0-9]+|)(:|)', 'IPAddandPortID')],
    },
    {
        path: '../../datasets/'
        logName: 'Zookeeper.log',
        savePath: '../../results/Zookeeper/'
        rex: [('(/|)([0-9]+\.){3}[0-9]+(:[0-9]+|)(:|)', 'IPAddandPortID')],
        log_format: '<Date> <Time> - <Level>  \[<Node>:<Component>@<Id>\] - <Content>',
    },
    {
        path: '../../datasets/'
        logName: 'Linux.log',
        savePath: '../../results/Linux/'
        rex: [('([0-9]+\.){3}[0-9]+', 'IPAdd')],
        log_format: '<Month> <Date> <Time> <Level> <Component>(\[<PID>\])?: <Content>',
    },
    {
        path: '../../datasets/'
        logName: 'Apache.log',
        savePath: '../../results/Apache/'
        rex: [],
        log_format: '\[<Time>\] \[<Level>\] <Content>',
    },
    {
        path: '../../datasets/'
        logName: 'Proxifier.log',
        savePath: '../../results/Proxifier/'
        rex: [],
        log_format: '\[<Time>\] <Program> - <Content>',
    },
    # {
    # 	path: '../../qdatastes'
    # 	logName: 'Spark.log',
    #     log_format: '<Date> <Time> <Level> <Component>: <Content>',
    # 	savePath: '../../results/Spark/
    # 	rex: [],
    # },
    # 		{
    # 	path: '../../qdatastes'
    # 	logName: 'Hive.log',
    # 	savePath: '../../results/Hive/
    # 	rex: [],
    # },
    # {
    # 	path: '../../qdatastes'
    # 	logName: 'Presto.log',
    # 	savePath: '../../results/Presto/
    # 	rex: [],
    # }
]


# benchmark_settings = {

#     # 'Spark': {
#     #     'log_file': 'Spark.log',
#     #     log_format: '<Date> <Time> <Level> <Component>: <Content>', 
#     #     'regex': [r'(\d+\.){3}\d+', r'\b[KGTM]?B\b', r'([\w-]+\.){2,}[\w-]+']
#     #     },
# }

for config in configs:
    print('\n=== Evaluation on %s ==='%dataset)
    indir = os.path.join(input_dir, os.path.dirname(setting['log_file']))
    log_file = os.path.basename(setting['log_file'])

    parser = MoLFI.LogParser(log_format=setting[log_format], indir=indir, outdir=output_dir, rex=setting['regex'])
    parser.parse(log_file)