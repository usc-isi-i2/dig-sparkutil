#!/usr/bin/env python

from pyspark import SparkContext
from pyspark import RDD
import json

def python_to_java_rdd(rdd, is_pair, is_json):
    if is_pair:
        if is_json:
            input = rdd.map(lambda x: x[0] + "\t" + json.dumps(x[1]))
        else:
            input = rdd.map(lambda x: x[0] + "\t" + x[1])
    else:
        if is_json:
            input = rdd.map(lambda x: json.dumps(x))
        else:
            input = rdd
    return input._to_java_object_rdd()

def java_to_python_rdd(sc, rdd, is_pair, is_json):
    jrdd = sc._jvm.SerDe.javaToPython(rdd)
    output = RDD(jrdd, sc)
    if is_pair:
        if is_json:
            return output.map(lambda x: (x.split("\t")[0], json.loads(x.split("\t")[1])))
        else:
            return output.map(lambda x: (x.split("\t")[0], x.split("\t")[1]))

    if is_json:
        return output.map(lambda x: json.loads(x))
    return output
