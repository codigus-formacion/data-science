from pyspark import SparkContext
from pyspark.streaming import StreamingContext

if __name__ == "__main__":
    
    # Create a local StreamingContext with two working thread and batch interval of 5 second
    sc = SparkContext("local[2]", "NetworkWordCount")
    sc.setLogLevel("ERROR")
    ssc = StreamingContext(sc, 5)
    
    ssc.checkpoint("checkpoint-window")

    # Create a DStream that will connect to hostname:port, like localhost:9999
    lines = ssc.socketTextStream("localhost", 9999)
    # Split each line into words
    words = lines.flatMap(lambda line: line.split(" "))
    # Count each word in each batch
    pairs = words.map(lambda word: (word, 1))
    # Reduce last 30 seconds of data, every 10 second
    windowedWordCounts = pairs.reduceByKeyAndWindow(lambda x, y: x + y, lambda x, y: x - y, 30, 10)
    # Print the first ten elements of each RDD generated in this DStream to the console
    windowedWordCounts.pprint()
    ssc.start()             # Start the computation
    ssc.awaitTermination()  # Wait for the computation to terminate