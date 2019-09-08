from pyspark import SparkContext


import json
import sys

sc = SparkContext()
# path = "C:\\Users\\Manish Dhankani\\Documents\\USC\\Spring 2019\\INF553 Data Mining\\Assignment\\1\\yelp_dataset\\review2.json"
path = sys.argv[1]
raw_data = sc.textFile(path)
dataset = raw_data.map(json.loads)
# dataset.persist()
a = dataset.map(lambda item: (item["business_id"], item["stars"]))
# print(type(a))
# a.foreach(print)

#path1 = "C:\\Users\\Manish Dhankani\\Documents\\USC\\Spring 2019\\INF553 Data Mining\\Assignment\\1\\yelp_dataset\\business1.json"
path1 = sys.argv[2]
raw_business_data = sc.textFile(path1)
b_dataset = raw_business_data.map(json.loads)
b = b_dataset.map(lambda item: (item["business_id"], item["city"]))
# b.foreach(print)
# print(type(dataset))
# print("Total number of records in reviews.json --> %i" % (dataset.count()))
# print("Total number of records in business.json --> %i" % (b_dataset.count()))

c = a.join(b)
# c.foreach(print)
# print(type(c))

d = c.map(lambda item: (item[1][1], item[1][0]))
# d.foreach(print)

init = (0, 0)
rdd1 = d.aggregateByKey(init, lambda sum, count: (sum[0] + count, sum[1] + 1),
                        lambda sum, count: (sum[0] + count[0], sum[1] + count[1]))
avgByKey = rdd1.mapValues(lambda sumCount: sumCount[0]/sumCount[1])
avgByKey.collect()
sorted_avgByKey = avgByKey.sortBy(lambda x: (-x[1], x[0]))
# print(sorted_avgByKey.collect())
out_data = sorted_avgByKey.collect()

# print(type(out_data))
with open(sys.argv[3], 'w', encoding="utf-8") as outfile:
    outfile.write('City,')
    outfile.write('Stars\n')
    i = 0
    for line in out_data:
        key = line[0]
        value = line[1]
        outfile.write(key +',')
        i += 1
        if(i!=len(out_data)):
            outfile.write(str(value) + '\n')
        else:
            outfile.write(str(value))

import time
out_data= {}
# method1
now = time.time()
sorted_avgByKey.collect()
i = 0
for key, val in sorted_avgByKey.collect():
    if(i==10):
        break
    print(key, val)
    i += 1
later = time.time()
diff = later - now
out_data["m1"] = diff
# print("Time for Method 1 is %i" % (diff))

# Method2
now1 = time.time()
l1 = sorted_avgByKey.take(10)
j = 0
for item in l1:
    key = item[0]
    value = item[1]
    j += 1
    print(key, ',')
    print(value)
# for key, val in sorted_avgByKey.collect():
#     print(key, val)
later1 = time.time()
diff1 = later1 - now1
out_data["m2"] = diff1
# print("Time for Method 2 is %i" %(diff1))
out_data["explanation"] = "Collect is an action that returns all the data from executors to diver program and might result in crash (if data exceeds memory capacity), so it involves high network traffic & printing the first 10 cities from entire data, while for method2 we only get the first 10 cities so the network traffic is less and hence less time compared to collect()"

with open(sys.argv[4], 'w') as outfile:
    json.dump(out_data, outfile)

sc.stop()