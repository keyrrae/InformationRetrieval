from __future__ import print_function
from busiMR import Business
from busiMR import Restaurant
from busiMR import attributeAccumulatorParam
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
        for attribute in restAttribList[i]:
            if attribute not in restAttribSet and attribute != u'Accepts Insurance':
                restAttribSet.append(attribute)
    print(restAttribSet)
    restAttribSet.sort()
    print(restAttribSet)
    restAttribSetBC = sc.broadcast(restAttribSet)

    # convert to (businessid, [attribute values])
    restMatrixRDD = restaurantRDD.map(lambda entry: Restaurant.toRowWithNA(entry, restAttribSetBC))

    restAttribValMatrix = restMatrixRDD.map(lambda entry: entry[1]).collect()

    restAttribValSet = Restaurant.collectAttributeValues(restAttribSet, restAttribValMatrix)

    Restaurant.printNonBinaryAttrValPairs(restAttribValSet)
    print(restAttribValSet)

    # convert to (businessid, attibute[i], attribute[i].value)
    restTupleListRDD = restaurantRDD.map(lambda entry: Restaurant.toTupleList(entry, restAttribSetBC))

    # print(restMatrixRDD.take(1))
    # print(restTupleListRDD.count())

    restTupleList = restTupleListRDD.collect()  # release later!

    restForALS = []  # release later!
    for item in restTupleList:
        restForALS.extend(item)

    restForALS_RDD = sc.parallelize(restForALS).filter(lambda line: line[2] != 'NA')




    '''
    print(restForALS_RDD.take(2))
    restaurantSample = restTupleListRDD.take(20)

    for i in xrange(20):
        print(restaurantSample[i])
    '''
    # collect all possible values of every attribute, using accumulator
    # unfortunately not working....
    '''
    attributeAccum = sc.accumulator(restAttribSetBC, attributeAccumulatorParam())
    restaurantRDD.map(lambda entry: (entry, restAttribSetBC)).foreach(lambda x: attributeAccum.add(x))

    print(type(attributeAccum))
    '''


    reviewRDD = sc.textFile("../../../data/yelp_academic_dataset_review.json")
    reviewRestaRDD = reviewRDD.map(Review.toString).filter(lambda line: Review.is_res(line, restaurantListBC))
    userReviewRestaRDD = reviewRestaRDD.map(Review.mapper).reduceByKey(Review.reducer)

    userRDD = sc.textFile("../../../data/yelp_academic_dataset_user.json")
    userRDD = userRDD.map(User.toString).map(User.getFriends)  # user friendlist

