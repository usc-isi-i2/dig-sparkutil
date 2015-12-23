#!/usr/bin/env python

# Filename: rddUtil.py

from logUtil import logging

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
