try:
    from pyspark import SparkContext
except:
    print "### NO PYSPARK"
from digSparkUtil.ESUtil import AddClusterId
from digSparkUtil.fileUtil import FileUtil
import argparse

def testSignature(sc,inputFileName,outputFileName,file_format):
    """
    :param sc: this is sparkcontext variable needed for spark
    :param inputFileName: inputdir to which cluster_id needs to be added
    :param outputFileName: outputdir
    :param file_format: text/seq
    """
    fUtil = FileUtil(sc)
    rdd = fUtil.load_file(inputFileName,file_format=file_format,data_type="json")
    addClusterId = AddClusterId()
    rdd = addClusterId.perform(rdd)
    fUtil.save_file(rdd,outputFileName,file_format='text',data_type='json')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-i","--inputFile",help = "input File path",required=True)
    parser.add_argument("-o","--output_dir",help = "output File path",required=True)
    parser.add_argument("--file_format",help = "file format text/sequence",default='sequence')

    args = parser.parse_args()
    sc = SparkContext(appName="AddClusterId")
    testSignature(sc,args.inputFile,args.output_dir,args.file_format)