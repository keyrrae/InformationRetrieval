from __future__ import print_function
from busiMR import Business
from reviewMR import Review
from userMR import User


from pyspark import SparkContext
from pyspark import mllib
from myRowMatrix import RowMatrixWithSimilarity


if __name__ == "__main__":
    sc = SparkContext('local')
    businessRDD = sc.textFile("../../../data/yelp_academic_dataset_business.json")

    rows = sc.parallelize([[1, 2, 3], [4, 5, 6], [7, 8, 9], [10, 11, 12]])
    mat = RowMatrixWithSimilarity(rows)

    restaurantRDD = businessRDD.map(Business.to_string).filter(Business.is_res)
    for item in businessRDD.take(10):
        print(item)
    restaurantList = restaurantRDD.map(Business.get_id).collect()
    restaurantListBC = sc.broadcast(restaurantList)
    restaurantRDD = restaurantRDD.map(Business.get_id_attributes)  # restaurant id : attributes

    restAttribList = restaurantRDD.map(Business.get_attribute_list).collect()

    for i in xrange(10):
        print(restAttribList[i])

    restAttribSet = list(restAttribList[0])  # list copy

    for i in xrange(len(restAttribList)):
        for j in xrange(len(restAttribSet)):
            if restAttribSet[j] not in restAttribList[i]:
                restAttribSet[j] = u''

    print(restAttribSet)

    reviewRDD = sc.textFile("../../../data/yelp_academic_dataset_review.json")
    reviewRestaRDD = reviewRDD.map(Review.toString).filter(lambda line: Review.is_res(line, restaurantListBC))
    userReviewRestaRDD = reviewRestaRDD.map(Review.mapper).reduceByKey(Review.reducer)

    userRDD = sc.textFile("../../../data/yelp_academic_dataset_user.json")
    userRDD = userRDD.map(User.toString).map(User.getFriends)  # user friendlist

