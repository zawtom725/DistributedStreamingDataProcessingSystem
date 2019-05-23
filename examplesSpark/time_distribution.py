from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
import argparse
import sys
import re


def time_distribution(sc, dstream):
	word_tuples = dstream.map(lambda line: line.split(",")[3]) \
				.filter(lambda time: time != 'timeStamp') \
				.map(lambda time: re.split('[- :]', time)[3]) \
				.map(lambda hour: (hour, 1))
	counts = word_tuples.reduceByKey(lambda a, b: a + b)

	# sum = counts.map(lambda count: (1, count[1])).reduceByKey(lambda a, b: a + b)
	# sum.pprint()(1,222)
	# total = word_tuples.reduce(lambda a, b : a + b)
	# total.pprint()
	# percentages = counts.map(lambda count : (count[0], float(count[1])/ sum.take(0)[1])
	# percentages.pprint()
	return counts

if __name__ == '__main__':
    # Get input/output files from user
    parser = argparse.ArgumentParser()

    parser.add_argument('--input_stream', help='Stream URL to pull data from')
    parser.add_argument('--input_stream_port', help='Stream port to pull data from')
	# parser.add_argument('--output_file', help='Output file name')

    args = parser.parse_args()

    # Setup Spark
    conf = SparkConf().setAppName("TimeDistribution")
    sc = SparkContext(conf=conf)
    ssc = StreamingContext(sc, 20)
    ssc.checkpoint('streaming_checkpoints')

    if args.input_stream and args.input_stream_port:
        dstream = ssc.socketTextStream(args.input_stream, int(args.input_stream_port))
        results = time_distribution(sc, dstream)
    else:
        print("Need input_stream and input_stream_port")
        sys.exit(1)

    results.saveAsTextFiles("/home/yanxu3/sparkTest", "txt")
    results.pprint()
	# results.show(24, False)

    ssc.start()
    ssc.awaitTermination()
