#!/usr/bin/env python

import json
import csv
import StringIO
import io
from dictUtil import as_dict

class FileUtil:
    def __init__(self, sparkContext):
        self.sc = sparkContext
        pass

    ## GENERIC

    ## Herein:
    ## file_format is in {text, sequence}
    ## data_type is in {csv, json}
    def load_file(self, filename, file_format='sequence', data_type='json', **kwargs):
        if data_type == "json":
            return self.load_json_file(filename, file_format, **kwargs)
        elif data_type == "csv":
            return self.load_csv_file(filename, file_format, **kwargs)
        else:
            raise ValueError("Unexpected file_format {}".format(file_format))

    def save_file(self, rdd, filename, file_format='sequence', data_type='json', **kwargs):
        if data_type == "json":
            return self.save_json_file(rdd, filename, file_format, **kwargs)
        elif data_type == "csv":
            return self.save_csv_file(rdd, filename, file_format, **kwargs)
        else:
            raise ValueError("Unexpected file_format {}".format(file_format))

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

    def load_json_file(self, filename, file_format, separator='\t'):
        """options is a dict or something coercible to dict
returns RDD of <uri, pyjson>
where pyjson is the python representation of the JSON object (e.g., dict)"""
        if file_format == "text":
            # each line is <key><tab><json>
            input_rdd = self.sc.textFile(filename).map(lambda x: FileUtil.__parse_json_line(x, separator))
        elif file_format == "sequence":
            # each element is <key><tab><json>
            input_rdd = self.sc.sequenceFile(filename).mapValues(lambda x: json.loads(x))
        else:
            raise ValueError("Unexpected file_format {}".format(file_format))
        return input_rdd

    @staticmethod
    def __dump_as_json(key, value, sep):
        return key + sep + json.dumps(value)

    def save_json_file(self, rdd, filename, file_format='sequence', separator='\t'):
        if file_format == "text":
            rdd.map(lambda (k, v): FileUtil.__dump_as_json(k, v, separator).saveAsTextFile(filename))
        elif file_format == "sequence":
            # whatever it is, key is retained
            rdd.mapValues(lambda x: json.dumps(x)).saveAsSequenceFile(filename)
        else:
            raise ValueError("Unexpected file_format {}".format(file_format))

    ## CSV

    def load_csv_file(self, filename, file_format, separator=','):
        """returns RDD, each row has all fields as list"""
        if file_format == "text":
            # http://stackoverflow.com/a/33864015/2077242
            input_rdd = self.sc.textFile(filename)

            def load_csv_record(line):
                input = StringIO.StringIO(line)
                reader = csv.reader(input, delimiter=separator)
                return reader.next()

            parsed_rdd = input_rdd.map(load_csv_record)
            return parsed_rdd

        elif file_format == "sequence":
            raise NotImplementedError("File_Format=sequence, data_type=csv")
        else:
            raise ValueError("Unexpected file_format {}".format(file_format))
        return input_rdd

    def save_csv_file(self, rdd, filename, file_format, separator=',', encoding='utf-8'):
        if file_format == "text":
            with io.open(filename, 'wb', encoding=encoding) as f:
                wrtr = csv.writer(f, delimiter=separator)
                
                def save_csv_record(line):
                    wrtr.writerow(line)

                rdd.foreach(save_csv_record)
                return filename

        elif file_format == "sequence":
            raise NotImplementedError("File_Format=sequence, data_type=csv")
        else:
            raise ValueError("Unexpected file_format {}".format(file_format))

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
    
