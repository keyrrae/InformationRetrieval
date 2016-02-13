from __future__ import print_function
from busiMR import Business
from reviewMR import Review
from userMR import User


from pyspark import SparkContext
from pyspark import mllib


if __name__ == "__main__":
    sc = SparkContext('local')
    businessRDD = sc.textFile("../../../data/yelp_academic_dataset_business.json")

    restaurantRDD = businessRDD.map(Business.to_string).filter(Business.is_res)
    for item in businessRDD.take(10):
        print(item)
    restaurantList = restaurantRDD.map(Business.get_id).collect()
    restaurantListBC = sc.broadcast(restaurantList)
    restaurantRDD = restaurantRDD.map(Business.get_id_attributes)
    for item in restaurantRDD.take(10):
        print(item)

    reviewRDD = sc.textFile("../../../data/yelp_academic_dataset_review.json")
    reviewRestaRDD = reviewRDD.map(Review.toString).filter(lambda line: Review.is_res(line, restaurantListBC))
    userReviewRestaRDD = reviewRestaRDD.map(Review.mapper).reduceByKey(Review.reducer)

    userRDD = sc.textFile("../../../data/yelp_academic_dataset_user.json")
    userRDD = userRDD.map(User.toString).map(User.getFriends)
