from pyspark import SparkContext

import json
import sys
# def task1(args, sc):
sc = SparkContext()
out_data = {}
# path = "C:\\Users\\Manish Dhankani\\Documents\\USC\\Spring 2019\\INF553 Data Mining\\Assignment\\1\\yelp_dataset\\review2.json"
# path = sys.argv[1]
path = sys.argv[1]
raw_data = sc.textFile(path)
review_data = raw_data.map(json.loads)
# review_data.persist()
# print(type(review_data))
n_review = review_data.count()
print("Total number of records in reviews.json --> %i" % (n_review))
out_data["n_review"] = n_review

all_dates = review_data.map(lambda item: item["date"], review_data)
n_reviews_2018 = all_dates.filter(lambda year: "2018" in year)
# n_reviews_2018.foreach(print)
n_review_2018 = n_reviews_2018.count()
print("Total number of reviews in year 2018 --> %i" % (n_review_2018))
out_data["n_review_2018"] = n_review_2018

total_users = review_data.map(lambda item: item["user_id"], review_data)
n_user = total_users.distinct().count()
print("The number of distinct users who wrote reviews --> %i" %(n_user))
out_data["n_user"] = n_user

user_review = review_data.map(lambda item: item["user_id"]).countByValue()
user_review_sort = sorted(user_review.items(),key=lambda k_v: (-k_v[1], k_v))
# print(type(user_review_sort))
# top10_user = user_review_sort[:10]
print(user_review_sort[:10])
out_data["top10_user"] = user_review_sort[:10]
# out_data["top10_user"].append(user_review_sort[:10])

total_business = review_data.map(lambda item: item["business_id"], review_data)
n_business = total_business.distinct().count()
print("The number of distinct business that have been reviewed --> %i" %(n_business))
out_data["n_business"] = n_business

# business_review = review_data.map(lambda item: item["business_id"]).countByValue()
business_review = review_data.map(lambda item: (item["business_id"], 1))
business_review = business_review.reduceByKey(lambda x,y: x+y)
# print(type(business_review))
# print(business_review.collect())
business_review_sort = business_review.sortBy(lambda x: (-x[1], x[0]))
# top10_business = business_review_sort.take(10)
print(business_review_sort.take(10))
out_data["top10_business"] = business_review_sort.take(10)
# out_data["top10_business"].append(business_review_sort.take(10))

with open(sys.argv[2], 'w') as outfile:
  print(json.dump(out_data, outfile, indent=1))

outfile.close()

sc.stop()
