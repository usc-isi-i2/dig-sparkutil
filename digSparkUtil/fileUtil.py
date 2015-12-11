#!/usr/bin/env python

import json

class FileUtil:
    def __init__(self, sparkContext):
        self.sc = sparkContext
        pass

    def load_json_file(self, filename, fileformat, options):
        if fileformat == "text":
            input_rdd = self.sc.textFile(filename).map(lambda x: FileUtil.__parse_json_line(x, FileUtil.__getOption(options, 'separator')))
        elif fileformat == "sequence":
            input_rdd = self.sc.sequenceFile(filename).mapValues(lambda x: json.loads(x))
        else:
            raise ValueError("Unexpected fileformat {}".format(fileformat))
        return input_rdd

    def save_json_file(self, rdd, filename, fileformat, options):
        if fileformat == "text":
            rdd.map(lambda (k, v): FileUtil.__dump_as_json(k, v, FileUtil.__getOption(options, 'separator'))).saveAsTextFile(filename)
        elif fileformat == "sequence":
            rdd.mapValues(lambda x: json.dumps(x)).saveAsSequenceFile(filename)
        else:
            raise ValueError("Unexpected fileformat {}".format(fileformat))

    ### TODO: load_csv_file
    ### TODO: save_csv_file

    @staticmethod
    def __getOption(optionsOrDict, optionName):
        """Should work for options stored in optparse.options, argparse.args, vanilla dict, or **kwargs"""
        try:
            return optionsOrDict.get(optionName)
        except:
            pass
        try:
            return optionsOrDict[optionName]
        except:
            raise ValueError("Unrecognized option {}".format(optionName))

    @staticmethod
    def __dump_as_json(key, value, sep):
        return key + sep + json.dumps(value)

    @staticmethod
    def __dump_as_csv(key, values, sep):
        line = key
        for part in values:
            line = line + sep + part
        return line

    @staticmethod
    def __parse_json_line(line, separator):
        line = line.strip()
        if len(line) > 0:
            line_elem = line.split(separator, 2)
            if len(line_elem) > 1:
                return line_elem[0], json.loads(line_elem[1])
            elif len(line_elem) == 1:
                return '', json.loads(line_elem[0])
