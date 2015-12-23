#!/usr/bin/env python

# Filename: rddUtil.py

import os
from logUtil import logging
import time
from datetime import timedelta
from random import randint

def limit_rdd(rdd, limit=None, randomSeed=1234):
    if not limit:
        return rdd
    else:
        # Originally:
        # rdd_ingest = sc.parallelize(rdd_ingest.take(limit))
        # However:
        # Because take/takeSample collects back to master, can needlessly 
        # create "task too large" condition
        # Therefore, instead, sample approximately 'limit' elements
        ratio = float(limit) / rdd.count()
        rdd_limited = rdd.sample(False, ratio, seed=randomSeed)
        logging.info("RDD limited to {} elements (requested: {})".format(rdd_limited.count(), limit))
        return rdd_limited

def show_partitioning(rdd, show=True):
    """Seems to be significantly more expensive on cluster than locally"""
    if show:
        partitionCount = rdd.getNumPartitions()
        try:
            valueCount = rdd.countApprox(1000, confidence=0.50)
        except:
            valueCount = -1
        try:
            name = rdd.name() or None
        except:
            pass
        name = name or "anonymous"
        logging.info("For RDD %s, there are %d partitions with on average %s values" % 
                     (name, partitionCount, int(valueCount/float(partitionCount))))

def debug_dump_rdd(rdd, 
                   debugLevel=0,
                   keys=True, 
                   listElements=False,
                   outputDir=None,
                   name=None):

    debugOutputRootDir = outputDir + '_debug'
    try:
        name = name or rdd.name() or None
    except:
        pass
    name = name or "anonymous-%d" % randint(10000,99999)
    outdir = os.path.join(debugOutputRootDir, name)

    if debugLevel > 0:
        startTime = time.time()
        # compute key count
        keyCount = None
        try:
            keyCount = rdd.keys().count() if keys else None
        except:
            pass
        # compute row count
        rowCount = None
        try:
            rowCount = rdd.count()
        except:
            pass
        # compute element count
        elementCount = None
        try:
            elementCount = rdd.mapValues(lambda x: len(x) if isinstance(x, (list, tuple)) else 0).values().sum() if listElements else None
        except:
            pass

        rdd.saveAsTextFile(outdir)
        endTime = time.time()
        elapsedTime = endTime - startTime
        logging.info("Wrote [%s] to outdir %r: [keys=%s, rows=%s, elements=%s]" % 
                     (str(timedelta(seconds=elapsedTime)), outdir, keyCount, rowCount, elementCount))
