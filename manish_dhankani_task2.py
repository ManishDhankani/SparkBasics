from pyspark import SparkContext
from pprint import pprint

import json
import time
import sys

sc = SparkContext()

raw_data = sc.textFile(sys.argv[1])
review_data = raw_data.map(json.loads)
# dataset.persist()
out_data = {}
now = time.time()
business_review = review_data.map(lambda item: (item["business_id"], 1))
business_review = business_review.reduceByKey(lambda x,y: x+y)
# print(type(business_review))
# print(business_review.collect())
# business_review_sort = business_review.sortBy(lambda x: (-x[1], x[0]))
# print(business_review.take(10))
default = {}
default_patition = business_review.getNumPartitions()
default["n_partition"] = default_patition
# print("The default partition is %i" %(default_patition))

def count_in_a_partition(iterator):
  yield sum(1 for _ in iterator)

l2 = business_review.mapPartitions(count_in_a_partition).collect()
later = time.time()
default["n_items"] = l2
default["exe_time"] = later - now
# print("The time for default partition is %i" %(later - now))
out_data["default"] = default
# Using hash Partition

customized = {}
n_partition = int(sys.argv[3])
#n_partition = 3
customized["n_partition"] = n_partition
now1 = time.time()
# custom_partition = review_data.map(lambda item: (item["business_id"], 1)
#                                         ).reduceByKey(lambda x,y: x+y
#                                                      ).partitionBy(n_partition,
#                                                                    lambda x: hash(x[0]) % n_partition).persist()
custom_partition = review_data.map(lambda item: (item["business_id"], 1)
                                        ).partitionBy(n_partition, lambda x: hash(x[0])% n_partition).reduceByKey(lambda x,y: x+y
                                                     )
# custom_partition = business_review.partitionBy(n_partition, lambda x: hash(x[0])%n_partition)
# custom_partition = custom_partition.sortBy(lambda x: (-x[1], x[0]))
# print("The custom partition is %i" %(n_custom_partition))

l3 = custom_partition.mapPartitions(count_in_a_partition).collect()
later1 = time.time()
customized["n_items"] = l3
customized["exe_time"] = later1 - now1
out_data["customized"] = customized
# print("The time for custom partition is %i" %(later - now))
out_data["explanation"] = "The main reason that custom partition helps is that by having all relevant data in one place(or few nodes) we reduce the overhead of shuffling. Having high number of partitions leads to the overhead of moving data over the network, whereas fewer partitions doesn't achieve optimum parallelism. A custom partition function is created for more fine-grained control over the partition scheme and hence it helps to achieve better parallelism, thus improving performance."
with open(sys.argv[2], 'w') as outfile:
    json.dump(out_data, outfile, indent=1)

sc.stop()