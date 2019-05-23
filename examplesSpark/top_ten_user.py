from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
import argparse
import sys
import re

def top_ten(sc, dstream):
	active_names = dstream.map(lambda line: line.split(",")[-3]) \
				.map(lambda name: (name, 1)) \
				.reduceByKey(lambda a, b: a+b) \
				.transform(lambda seq: seq.sortBy(lambda x: x[1], ascending=False))
	return active_names

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
    ssc = StreamingContext(sc, 100)
    ssc.checkpoint('streaming_checkpoints')

    if args.input_stream and args.input_stream_port:
        dstream = ssc.socketTextStream(args.input_stream,
                                       int(args.input_stream_port))
        results = top_ten(sc, dstream)
    else:
        print("Need input_stream and input_stream_port")
        sys.exit(1)

    # results.saveAsTextFiles(args.output)
    results.pprint()

    ssc.start()
    ssc.awaitTermination()
