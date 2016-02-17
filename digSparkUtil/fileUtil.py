#!/usr/bin/env python

from __future__ import print_function
import sys
import json
import csv
import StringIO
import io
from dictUtil import as_dict, merge_dicts
import urllib
# import jq
from itertools import izip
from logUtil import logging
# for manifest introspection only
import inspect

from pyspark import SparkContext

class FileUtil(object):
    def __init__(self, sparkContext):
        self.sc = sparkContext

    ## Support for entries into manifest
    # any entry created thus
    # should have spark_context, name of caller, module of caller
    # untested: do not use
    def makeEntry(self, **kwargs):
        entry = dict(**kwargs)
        entry["spark_context"] = self.sc
        op = kwargs.get("operation", None)
        if not op:
            try:
                st = inspect.stack()
                # stack exists
                if len(st)>=2:
                    # look up one stack frame, retrieve the function name[3]
                    op = st[1][3]
                # stack frame memory leak could be very bad, so be careful
                del st
            except:
                pass

        mdl = kwargs.get("module", None)
        if not mdl:
            try:
                st = inspect.stack()
                # stack exists
                if len(st)>=2:
                    # look up one stack frame, retrieve the module it belongs to
                    mdl = inspect.getmodule(st[0]).__name__
                # stack frame memory leak could be very bad, so be careful
                del st
            except:
                pass
        entry["module"] = mdl

        return entry

    ## GENERIC

    ## Herein:
    ## file_format is in {text, sequence}
    ## data_type is in {csv, json, jsonlines(=keyless)}
    
    def load_file(self, filename, file_format='sequence', data_type='json', **kwargs):
        try:
            handlerName = FileUtil.load_dispatch_table[(file_format, data_type)]
            handler = getattr(self, handlerName)
            rdd = handler(filename, **kwargs)
            # TBD: return (rdd, manifestEntry)
            # entry = self.makeEntry(input_filename=filename,
            #                        input_file_format=file_format,
            #                        input_data_type=data_type)
            # return (rdd, entry)
            #logging.info("Loaded {}/{} file {}: {} elements".format(file_format, data_type, filename, rdd.count()))
            return rdd
        except KeyError: 
            raise NotImplementedError("File_Format={}, data_type={}".format(file_format, data_type))

    load_dispatch_table = {("sequence", "json"):  "_load_sequence_json_file",
                           ("sequence", "csv"):   "_load_sequence_csv_file",
                           ("text", "json"):      "_load_text_json_file",
                           ("text", "jsonlines"): "_load_text_jsonlines_file",
                           ("text", "csv"):       "_load_text_csv_file"}
    
    def _load_sequence_json_file(self, filename, **kwargs):
        rdd_input = self.sc.sequenceFile(filename)
        rdd_json = rdd_input.mapValues(lambda x: json.loads(x))
        return rdd_json

    def _load_text_json_file(self, filename, separator='\t', **kwargs):
        # rdd_input = self.sc.textFile(filename)
        # rdd_json = rdd_input.map(lambda x: FileUtil.__parse_json_line(x, separator))
        rdd_strings = self.sc.textFile(filename)
        rdd_split = rdd_strings.map(lambda line: tuple(line.split(separator, 1)))
        def tryJson(v):
            try:
                j = json.loads(v)
                return j
            except Exception as e:
                print("failed [{}] on {}".format(str(e), v), file=sys.stderr)
        rdd_json = rdd_split.mapValues(lambda v: tryJson(v))
        return rdd_json

    def _load_text_jsonlines_file(self, filename, keyPath='.uri', **kwargs):
        rdd_strings = self.sc.textFile(filename)
        def tryJson(line):
            try:
                obj = json.loads(line)
                # We ignore all but the first occurrence of key
                try:
                    # key = jq.jq(keyPath).transform(obj, multiple_output=False)
                    key = obj["uri"]
                except:
                    key = None
                if key:
                    # i.e., a paired RDD
                    return (key, obj)
                else:
                    raise ValueError("No key (per {}) in line {}".format(keyPath, line))
            except Exception as e:
                print("failed [{}] on {}".format(str(e), line), file=sys.stderr)
        rdd_json = rdd_strings.map(lambda line: tryJson(line))
        return rdd_json


    def _load_sequence_csv_file(self, filename, **kwargs):
        """Should emulate text/csv"""
        raise NotImplementedError("File_Format=sequence, data_type=csv")

    def _load_text_csv_file(self, filename, separator=',', **kwargs):
        """Return a pair RDD where key is taken from first column, remaining columns are named after their column id as string"""
        rdd_input = self.sc.textFile(filename)

        def load_csv_record(line):
            input_stream = StringIO.StringIO(line)
            reader = csv.reader(input_stream, delimiter=',')
            # key in first column, remaining columns 1..n become dict key values
            payload = reader.next()
            key = payload[0]
            rest = payload[1:]
            # generate dict of "1": first value, "2": second value, ...
            d = {}
            for (cell,i) in izip(rest, range(1,1+len(rest))):
                d[str(i)] = cell
            # just in case, add "0": key
            d["0"] = key
            return (key, d)

        rdd_parsed = rdd_input.map(load_csv_record)
        return rdd_parsed

    ## SAVE

    def save_file(self, rdd, filename, file_format='sequence', data_type='json', **kwargs):
        try:
            handlerName = FileUtil.save_dispatch_table[(file_format, data_type)]
            handler = getattr(self, handlerName)
            rdd = handler(rdd, filename, **kwargs)
            # TBD: return (rdd, manifestEntry)
            # entry = self.makeEntry(output_filename=filename,
            #                        output_file_format=file_format,
            #                        output_data_type=data_type)
            # return (rdd, entry)
            return rdd
        except KeyError: 
            raise NotImplementedError("File_Format={}, data_type={}".format(file_format, data_type))

    save_dispatch_table = {("sequence", "json"): "_save_sequence_json_file",
                           ("sequence", "csv"):  "_save_sequence_csv_file",
                           ("text", "json"):     "_save_text_json_file",
                           ("text", "csv"):      "_save_text_csv_file"}

    def _save_sequence_json_file(self, rdd, filename, separator='\t', **kwargs):
        # regardless of whatever it is, key is retained
        rdd.mapValues(lambda x: json.dumps(x)).saveAsSequenceFile(filename)
        return filename

    def _save_text_json_file(self, rdd, filename, separator='\t', **kwargs):
        rdd_json = rdd.map(lambda (k, v): FileUtil.__dump_as_json(k, v, separator))
        # this saves the (<uri>, <serialized_json_string>) as as text repn
        # perhaps a regular readable text file uri<separator>JSON will be more useful?
        rdd_json.saveAsTextFile(filename)
        return filename

    def _save_text_csv_file(self, rdd, filename, separator='\t', encoding='utf-8', **kwargs):
        with io.open(filename, 'wb', encoding=encoding) as f:
            wrtr = csv.writer(f, delimiter=separator)
                
            def save_csv_record(line):
                wrtr.writerow(line)

            rdd.foreach(save_csv_record)
        return filename

    def _save_sequence_csv_file(self, rdd, filename, separator='\t', **kwargs):
        raise NotImplementedError("File_Format=sequence, data_type=csv")

    ## JSON

    @staticmethod
    def __parse_json_line(line, separator):
        line = line.strip()
        if len(line) > 0:
            line_elem = line.split(separator, 2)
            if len(line_elem) > 1:
                return line_elem[0], json.loads(line_elem[1])
            elif len(line_elem) == 1:
                return '', json.loads(line_elem[0])

    @staticmethod
    def __dump_as_json(key, value, sep):
        return key + sep + json.dumps(value)

    @staticmethod
    def get_json_config(config_spec):
        # if it's a dict, or coercible to a dict, return the dict
        try:
            return as_dict(config_spec)
        except TypeError:
            pass
        # Not a dict
        config_file = None
        if config_spec.startswith("http"):
            # URL: fetch it
            config_file = urllib.urlopen(config_spec)
        else:
            # string: open file with that name
            config_file = open(config_spec)
        config = json.load(config_file)
        # Close any open files
        try:
            config_file.close()
        except:
            pass
        return config
    
    @staticmethod
    def get_config(config_spec):
        """Like get_json_config but does not parse result as JSON"""
        config_file = None
        if config_spec.startswith("http"):
            # URL: fetch it
            config_file = urllib.urlopen(config_spec)
        else:
            # string: open file with that name
            config_file = open(config_spec)
        config = json.load(config_file)
        # Close any open files
        try:
            config_file.close()
        except:
            pass
        return config

##################################################################

import argparse

def main(argv=None):
    '''TEST ONLY: this is called if run from command line'''

    parser = argparse.ArgumentParser()
    parser.add_argument('-i','--input_file', required=True)
    parser.add_argument('--input_file_format', default='sequence')
    parser.add_argument('--input_data_type', default='json')
    parser.add_argument('--input_separator', default='\t')
    parser.add_argument('-o','--output_dir', required=True)
    parser.add_argument('--output_file_format', default='sequence')
    parser.add_argument('--output_data_type', default='json')
    parser.add_argument('--output_separator', default='\t')
    args=parser.parse_args()

    # can be inconvenient to specify tab on the command line
    args.input_separator = "\t" if args.input_separator=='tab' else args.input_separator
    args.output_separator = "\t" if args.output_separator=='tab' else args.output_separator

    sc = SparkContext(appName="fileUtil")
    fUtil = FileUtil(sc)

    ## CONFIG LOAD
    input_kwargs = {"file_format": args.input_file_format,
                   "data_type": args.input_data_type}
    parse_kwargs = {"separator": args.input_separator}
    load_kwargs = merge_dicts(input_kwargs, parse_kwargs)
    
    ## LOAD
    rdd = fUtil.load_file(args.input_file, **load_kwargs)

    ## CONFIG SAVE
    output_kwargs = {"file_format": args.output_file_format,
                     "data_type": args.output_data_type}
    emit_kwargs = {"separator": args.output_separator}
    save_kwargs = merge_dicts(output_kwargs, emit_kwargs)

    ## SAVE
    fUtil.save_file(rdd, args.output_dir, **save_kwargs)

if __name__ == "__main__":
    """
        Usage: tokenizer.py [input] [config] [output]
    """
    main()
