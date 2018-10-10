#!/usr/bin/env python

import sys
sys.path.append('../')
from logparser import SHISO, evaluator
import os
import pandas as pd


input_dir = '../../datasets/'  # The input directory of log file
output_dir = 'SHISO_result/' # The output directory of parsing results

benchmark_settings = {
    # 'HDFS': {
    #     'log_file': 'HDFS.log',
    #     'log_format': '<Date> <Time> <Pid> <Level> <Component>: <Content>',
    #     'regex': [r'blk_-?\d+', r'(\d+\.){3}\d+(:\d+)?'],
    #     'maxChildNum': 4,
    #     'mergeThreshold': 0.1,
    #     'formatLookupThreshold': 0.3,
    #     'superFormatThreshold': 0.85
    #     },

    # 'Zookeeper': {
    #     'log_file': 'Zookeeper.log',
    #     'log_format': '<Date> <Time> - <Level>  \[<Node>:<Component>@<Id>\] - <Content>',
    #     'regex': [r'(/|)(\d+\.){3}\d+(:\d+)?'],
    #     'maxChildNum': 4,
    #     'mergeThreshold': 0.003,
    #     'formatLookupThreshold': 0.3,
    #     'superFormatThreshold': 0.85
    #     },

    # 'BGL': {
    #     'log_file': 'BGL.log',
    #     'log_format': '<Label> <Timestamp> <Date> <Node> <Time> <NodeRepeat> <Type> <Component> <Level> <Content>',
    #     'regex': [r'core\.\d+'],
    #     'maxChildNum': 4,
    #     'mergeThreshold': 0.005,
    #     'formatLookupThreshold': 0.3,
    #     'superFormatThreshold': 0.85
    #     },

    # 'HPC': {
    #     'log_file': 'HPC.log',
    #     'log_format': '<LogId> <Node> <Component> <State> <Time> <Flag> <Content>',
    #     'regex': [r'=\d+'],
    #     'maxChildNum': 3,
    #     'mergeThreshold': 0.003,
    #     'formatLookupThreshold': 0.6,
    #     'superFormatThreshold': 0.4
    #     },

    # 'Linux': {
    #     'log_file': 'Linux.log',
    #     'log_format': '<Month> <Date> <Time> <Level> <Component>(\[<PID>\])?: <Content>',
    #     'regex': [r'(\d+\.){3}\d+', r'\d{2}:\d{2}:\d{2}'],
    #     'maxChildNum': 3,
    #     'mergeThreshold': 0.005,
    #     'formatLookupThreshold': 0.3,
    #     'superFormatThreshold': 0.4
    #     },

    # 'Apache': {
    #     'log_file': 'Apache.log',
    #     'log_format': '\[<Time>\] \[<Level>\] <Content>',
    #     'regex': [r'(\d+\.){3}\d+'],
    #     'maxChildNum': 4,
    #     'mergeThreshold': 0.002,
    #     'formatLookupThreshold': 0.3,
    #     'superFormatThreshold': 0.85
    #     },

    'Proxifier': {
        'log_file': 'Proxifier.log',
        'log_format': '\[<Time>\] <Program> - <Content>',
        'regex': [r'<\d+\ssec', r'([\w-]+\.)+[\w-]+(:\d+)?', r'\d{2}:\d{2}(:\d{2})*', r'\b[KGTM]?B\b'],
        'maxChildNum': 4,
        'mergeThreshold': 0.002,
        'formatLookupThreshold': 0.3,
        'superFormatThreshold': 0.85
        }
}

# bechmark_result = []
for dataset, setting in benchmark_settings.iteritems():
    print('\n=== Evaluation on %s ==='%dataset)
    indir = os.path.join(input_dir, os.path.dirname(setting['log_file']))
    log_file = os.path.basename(setting['log_file'])

    parser = SHISO.LogParser(log_format=setting['log_format'], indir=indir, outdir=output_dir, rex=setting['regex'],
                            maxChildNum=setting['maxChildNum'], mergeThreshold=setting['mergeThreshold'],
                            formatLookupThreshold=setting['formatLookupThreshold'], superFormatThreshold=setting['superFormatThreshold'])
    parser.parse(log_file)
    
    # F1_measure, accuracy = evaluator.evaluate(
    #                        groundtruth=os.path.join(indir, log_file + '_structured.csv'),
    #                        parsedresult=os.path.join(output_dir, log_file + '_structured.csv')
    #                        )
    # bechmark_result.append([dataset, F1_measure, accuracy])


# print('\n=== Overall evaluation results ===')
# df_result = pd.DataFrame(bechmark_result, columns=['Dataset', 'F1_measure', 'Accuracy'])
# df_result.set_index('Dataset', inplace=True)
# print(df_result)
# df_result.T.to_csv('SHISO_bechmark_result.csv')



configs = [
    {
        path: '../../datasets/'
        logName: 'BGL.log',
        savePath: '../../results/BGL/'
        rex: [('core\.[0-9]*', 'coreNum')],
        log_format: '<Label> <Timestamp> <Date> <Node> <Time> <NodeRepeat> <Type> <Component> <Level> <Content>',
        maxChildNum: 4,
        mergeThreshold: 0.1,
        formatLookupThreshold: 0.3,
        superFormatThreshold: 0.85
    },
    {
        path: '../../datasets/'
        logName: 'HPC.log',
        savePath: '../../results/HPC/'
        rex: [('([0-9]+\.){3}[0-9]', 'IPAdd'), ('node-[0-9]+', 'nodeNum')],
        log_format: '<LogId> <Node> <Component> <State> <Time> <Flag> <Content>',
        maxChildNum: 4,
        mergeThreshold: 0.1,
        formatLookupThreshold: 0.3,
        superFormatThreshold: 0.85
    },
    {
        path: '../../datasets/'
        logName: 'HDFS.log',
        log_format: '<Date> <Time> <Pid> <Level> <Component>: <Content>',
        savePath: '../../results/HDFS/'
        rex: [('blk_(|-)[0-9]+', 'blkID'), ('(/|)([0-9]+\.){3}[0-9]+(:[0-9]+|)(:|)', 'IPAddandPortID')],
        maxChildNum: 4,
        mergeThreshold: 0.1,
        formatLookupThreshold: 0.3,
        superFormatThreshold: 0.85
    },
    {
        path: '../../datasets/'
        logName: 'Zookeeper.log',
        savePath: '../../results/Zookeeper/'
        rex: [('(/|)([0-9]+\.){3}[0-9]+(:[0-9]+|)(:|)', 'IPAddandPortID')],
        log_format: '<Date> <Time> - <Level>  \[<Node>:<Component>@<Id>\] - <Content>',
        maxChildNum: 4,
        mergeThreshold: 0.1,
        formatLookupThreshold: 0.3,
        superFormatThreshold: 0.85
    },
    {
        path: '../../datasets/'
        logName: 'Linux.log',
        savePath: '../../results/Linux/'
        rex: [('([0-9]+\.){3}[0-9]+', 'IPAdd')],
        log_format: '<Month> <Date> <Time> <Level> <Component>(\[<PID>\])?: <Content>',
        maxChildNum: 4,
        mergeThreshold: 0.1,
        formatLookupThreshold: 0.3,
        superFormatThreshold: 0.85
    },
    {
        path: '../../datasets/'
        logName: 'Apache.log',
        savePath: '../../results/Apache/'
        rex: [('(\d+\.){3}\d+', '')],
        log_format: '\[<Time>\] \[<Level>\] <Content>',
        maxChildNum: 4,
        mergeThreshold: 0.1,
        formatLookupThreshold: 0.3,
        superFormatThreshold: 0.85
    },
    {
        path: '../../datasets/'
        logName: 'Proxifier.log',
        savePath: '../../results/Proxifier/'
        rex: [('<\d+\ssec', ''), ('([\w-]+\.)+[\w-]+(:\d+)?', ''), ('\d{2}:\d{2}(:\d{2})*', ''), ('\b[KGTM]?B\b', '')],
        log_format: '\[<Time>\] <Program> - <Content>',
        maxChildNum: 4,
        mergeThreshold: 0.002,
        formatLookupThreshold: 0.3,
        superFormatThreshold: 0.85
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

for config in configs:
    parser = MoLFI.LogParser(log_format=config["log_format"], indir=config["path"],
                             outdir=config["savePath"], rex=setting['rex'])
    parser.parse(config["logName"])