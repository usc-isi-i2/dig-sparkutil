# dig-sparkutil
python utilities for incorporating DIG components into Spark workflows

Note: seq2tsv inclusion forced dependency on python hadoop lib
from https://github.com/matteobertozzi/Hadoop
for now this is forked to
https://github.com/usc-isi-i2/Hadoop

##################################################################

example of running test suites

rm -rf /tmp/csv04
python fileUtil.py -i tests/text04/input/input04.csv --input_file_format 'text' --input_data_type 'csv' --input_separator "," -o '/tmp/csv04' --output_file_format text --output_data_type 'json' --output_separator tab
cat /tmp/csv04/part*


####Dependencies for lsh clustering : <br />
1. pip install digSparkUtil <br />
    This is the code for reading the files into spark rdds <br />
2. pip install digTokenizer <br />
    This is the code for generating tokens, tweak tokenizer configuration as per the requirement. <br />
3. pip install digLshCLustering <br />
    This does the lsh clustering based on the tokens generated.<br />

