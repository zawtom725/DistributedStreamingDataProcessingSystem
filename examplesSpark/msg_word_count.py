from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
import argparse
import sys
import re


# [["1","2"],[]]
# def word_count(sc, dstream):
# 	counts = dstream.flatMap(lambda line: line.split(",")[-1]) \
# 				.filter(lambda msg: msg != 'msg') \
# 				.map(lambda )
# 				.map(lambda sentence: re.split('[- : . ; , ?]', sentence)) \
# 				# .flatMap(lambda word_list: word_list) \
# 				# .map(lambda word: (word, 1)) \
# 				# .reduceByKey(lambda a, b: a+b)
# 	return counts
def word_count(sc, dstream):
	counts = dstream.map(lambda line: line.split(",")[-1]) \
				.filter(lambda msg: msg != 'msg') \
				.map(lambda sentence: re.split('[- : . ; , ?]', sentence)) \
				.flatMap(lambda word_list: word_list) \
				.filter(lambda word: len(word)) \
				.map(lambda word: (word, 1)) \
				.reduceByKey(lambda a, b: a+b)
	return counts

if __name__ == '__main__':
    # Get input/output files from user
    parser = argparse.ArgumentParser()

    parser.add_argument('--input_stream', help='Stream URL to pull data from')
    parser.add_argument('--input_stream_port', help='Stream port to pull data from')

    # parser.add_argument('output', help='Directory to save DStream results to')
    args = parser.parse_args()

    # Setup Spark
    conf = SparkConf().setAppName("CountMsgWords")
    sc = SparkContext(conf=conf)
    ssc = StreamingContext(sc, 10)
    ssc.checkpoint('streaming_checkpoints')

    if args.input_stream and args.input_stream_port:
        dstream = ssc.socketTextStream(args.input_stream,
                                       int(args.input_stream_port))
        results = word_count(sc, dstream)
    else:
        print("Need input_stream and input_stream_port")
        sys.exit(1)

    # results.saveAsTextFiles(args.output)
    results.pprint()

    ssc.start()
    ssc.awaitTermination()
