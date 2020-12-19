from pyspark import SparkContext
from pyspark.streaming import StreamingContext

# Create a local StreamingContext with two working thread and batch interval of 1 second
sc = SparkContext("local[2]", "TwitterCovidCorrelation")  # 2 x local threads, name
ssc = StreamingContext(sc, 5)  # update interval

# Create a DStream that will connect to hostname:port, like localhost:9999
twit = ssc.socketTextStream("localhost", 9999)

ssc.start()             # Start the computation
ssc.awaitTermination()  # Wait for the computation to terminate
