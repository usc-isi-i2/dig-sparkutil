#!/usr/bin/env python

import javaToPythonSpark
import json
import urllib
import framer

class Workflow:
    def __init__(self, spark_context):
        self.sc = spark_context

    @staticmethod
    def read_json_file(filename):
        print "Read file:", filename
        if filename.find("http") == 0:
            file_handle = urllib.urlopen(filename)
        else:
            file_handle = open(filename)
        data = json.load(file_handle)
        file_handle.close()
        return data

    def batch_read_csv(self, filename):
        return self.sc.newAPIHadoopFile(filename,
                        "edu.isi.karma.mapreduce.inputformat.CSVBatchTextInputFormat",
                        "org.apache.hadoop.io.NullWritable", "org.apache.hadoop.io.Text")\
                .map(lambda x: (str(x[0]), x[1]))

    def run_karma(self, rdd, model, base, root, context, data_type="json", additional_settings={}):
        karma_settings = {}
        karma_settings["karma.input.type"] = data_type
        karma_settings["base.uri"] = base
        karma_settings["rdf.generation.root"] = root
        karma_settings["model.uri"] = model
        karma_settings["is.model.in.json"] = "true"
        karma_settings["context.uri"] = context
        karma_settings["is.root.in.json"] = "true"
        karma_settings["read.karma.config"] = "false"
        karma_settings["rdf.generation.disable.nesting"] = "true"
        for name in additional_settings:
            karma_settings[name] = additional_settings[name]

        is_json = False
        if data_type == "json":
            is_json = True

        input_rdd_java = javaToPythonSpark.python_to_java_rdd(rdd, True, is_json)
        output_rdd_java = self.sc._jvm.edu.isi.karma.spark.KarmaDriver.applyModel(self.sc._jsc,
                                                                                  input_rdd_java,
                                                                                  json.dumps(karma_settings),
                                                                                  1000)

        output_rdd = javaToPythonSpark.java_to_python_rdd(self.sc, output_rdd_java, True, True)
        return output_rdd

    def reduce_rdds(self, *rdd_list):
        all_rdd = rdd_list[0]
        for rdd in rdd_list[1:]:
            all_rdd = all_rdd.union(rdd)
        reduced_java = self.sc._jvm.edu.isi.karma.spark.JSONReducerDriver.reduceJSON(self.sc._jsc,
                                                                        javaToPythonSpark.python_to_java_rdd(all_rdd, True, True))
        return javaToPythonSpark.java_to_python_rdd(self.sc, reduced_java, True, True)

    def apply_context(self, rdd, context, context_url):
        rdd_java = javaToPythonSpark.python_to_java_rdd(rdd, True, True)
        output_java = self.sc._jvm.edu.isi.karma.spark.JSONContextDriver.applyContext(self.sc._jsc,
                                                                   rdd_java,
                                                                   json.dumps(context),
                                                                   context_url)
        return javaToPythonSpark.java_to_python_rdd(self.sc, output_java, True, True)

    def apply_framer(self, rdd, types, frames):
        type_to_rdd_json = framer.partition_rdd_on_types(rdd, types)
        output = {}
        for frame in frames:
            frame_json_data = self.read_json_file(frame["url"])
            out_framer = framer.frame_json(frame_json_data, type_to_rdd_json)
            output[frame["name"]] = out_framer
        return output

    @staticmethod
    def __convert_list_to_tuple(some_dictionary):
        # print "\n\nGot", type(some_dictionary), ":", some_dictionary
        if isinstance(some_dictionary, dict):
            for key in some_dictionary:
                value = some_dictionary[key]
                # print "\tGot value:", type(value), ":", value
                if isinstance(value, list):
                    new_value = list()
                    for item in value:
                        new_value.append(Workflow.__convert_list_to_tuple(item))
                    value = tuple(new_value)
                elif isinstance(value, dict):
                    value = Workflow.__convert_list_to_tuple(value)
                some_dictionary[key] = value
        elif isinstance(some_dictionary, list):
            new_value = list()
            for item in some_dictionary:
                new_value.append(Workflow.__convert_list_to_tuple(item))
            some_dictionary = tuple(new_value)
        return some_dictionary

    @staticmethod
    def save_rdd_to_es(rdd, host, port, index):
        rdd = rdd.mapValues(lambda x: Workflow.__convert_list_to_tuple(x))
        if rdd is not None and not rdd.isEmpty():
            rdd = rdd.mapValues(lambda x: Workflow.__convert_list_to_tuple(x))

            print "Save to ES:", host, port, index
            es_write_conf = {
                "es.nodes": host,
                "es.port": port,
                "es.resource": index
            }
            rdd.saveAsNewAPIHadoopFile(
                path='-',
                outputFormatClass="org.elasticsearch.hadoop.mr.EsOutputFormat",
                keyClass="org.apache.hadoop.io.NullWritable",
                valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable",
                conf=es_write_conf)

