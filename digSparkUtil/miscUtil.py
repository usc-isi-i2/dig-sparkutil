#!/usr/bin/env python

# Filename: miscUtil.py

from datetime import datetime

def seconds_since_epoch():
    return int(round((datetime.utcnow() - datetime(1970, 1, 1)).total_seconds()))

def limit_rdd(rdd, limit=None, randomSeed=1234):
    if not limit:
        return rdd
    else:
        # Because take/takeSample collects back to master, can create "task too large" condition
        # rdd_ingest = sc.parallelize(rdd_ingest.take(limit))
        # Instead, generate approximately 'limit' rows
        ratio = float(limit) / rdd.count()
        rdd_limited = rdd.sample(False, ratio, seed=randomSeed)
        return rdd_limited
