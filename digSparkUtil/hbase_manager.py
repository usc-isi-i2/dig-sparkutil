__author__ = 'amandeep'


class HbaseManager(object):
    def __init__(self, sc, conf, hbase_hostname, hbase_tablename):
        self.name = "ES2HBase"
        self.sc = sc
        self.conf = conf
        self.hbase_conf = {"hbase.zookeeper.quorum": hbase_hostname}
        self.hbase_table = hbase_tablename

    def rdd2hbase(self, data_rdd):
        self.hbase_conf['hbase.mapred.outputtable'] = self.hbase_table
        self.hbase_conf['mapreduce.outputformat.class'] = "org.apache.hadoop.hbase.mapreduce.TableOutputFormat"
        self.hbase_conf['mapreduce.job.output.key.class'] = "org.apache.hadoop.hbase.io.ImmutableBytesWritable"
        self.hbase_conf['mapreduce.job.output.value.class'] = "org.apache.hadoop.io.Writable"

        key_conv = "org.apache.spark.examples.pythonconverters.StringToImmutableBytesWritableConverter"
        value_conv = "org.apache.spark.examples.pythonconverters.StringListToPutConverter"
        print self.hbase_conf

        # datamap = data_rdd.flatMap(HbaseManager.create_ads_tuple)

        data_rdd.saveAsNewAPIHadoopDataset(
            conf=self.hbase_conf,
            keyConverter=key_conv,
            valueConverter=value_conv)

    def read_hbase_table(self):
        self.hbase_conf['hbase.mapreduce.inputtable'] = self.hbase_table
        key_conv = "org.apache.spark.examples.pythonconverters.ImmutableBytesWritableToStringConverter"
        value_conv = "org.apache.spark.examples.pythonconverters.HBaseResultToStringConverter"

        hbase_rdd = self.sc.newAPIHadoopRDD("org.apache.hadoop.hbase.mapreduce.TableInputFormat",
                                             "org.apache.hadoop.hbase.io.ImmutableBytesWritable",
                                             "org.apache.hadoop.hbase.client.Result",
                                             keyConverter=key_conv,
                                             valueConverter=value_conv,
                                             conf=self.hbase_conf)
        # hbase_rdd = hbase_rdd.flatMapValues(lambda v: v.split("\n")).mapValues(json.loads)
        return hbase_rdd
