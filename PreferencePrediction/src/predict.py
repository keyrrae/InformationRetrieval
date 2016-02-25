from __future__ import print_function
from busiMR import Business
from busiMR import Restaurant
from busiMR import attributeAccumulatorParam
from reviewMR import Review
from userMR import User

import os

from pyspark import SparkContext
from pyspark import mllib
from pyspark.mllib.linalg.distributed import RowMatrix
from pyspark.mllib.recommendation import ALS, MatrixFactorizationModel, Rating
from pyspark.mllib.clustering import PowerIterationClustering, PowerIterationClusteringModel
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

    # construct two dictionaries to store businessID and corresponding integers mutual translation
    restGetNum, restGetID = Restaurant.buildID_NumReferences(restaurantList)
    restGetNumBC = sc.broadcast(restGetNum)
    restGetIDBC = sc.broadcast(restGetID)



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
    print(len(restAttribSet))
    # construct two dictionaries to store attribute name and the corresponding integers translation.
    attriGetNum, attriGetName = Restaurant.buildID_NumReferences(restAttribSet)

    restAttribSetBC = sc.broadcast(restAttribSet)
    attriGetNumBC = sc.broadcast(attriGetNum)
    for i in xrange(len(restAttribSet)):
        if restAttribSet[i] in [u'Ages Allowed', u'Alcohol', u'Attire', u'BYOB/Corkage', u'Noise Level',
                                u'Price Range', u'Smoking', u'Wi-Fi']:
            print((i, restAttribSet[i]))

    # convert to (businessid, [attribute values])
    restMatrixRDD = restaurantRDD.map(lambda entry: Restaurant.toRowWithNA(entry, restAttribSetBC))
    print(restMatrixRDD.count())
    print(restMatrixRDD.first())

    # replace nominal values in attributes with numeric values
    restMatNumericRDD = restMatrixRDD.map(lambda entry: Restaurant.toNumeric(entry, restGetNumBC, restAttribSetBC))
    print(restMatNumericRDD.first())
    restAttribValMatrix = restMatrixRDD.map(lambda entry: entry[1]).collect()

    restAttribValSet = Restaurant.collectAttributeValues(restAttribSet, restAttribValMatrix)

    Restaurant.printNonBinaryAttrValPairs(restAttribValSet)
    print(restAttribValSet)

    # convert to (businessid, attibute[i], attribute[i].value)
    restTupleListRDD = restMatNumericRDD.map(Restaurant.toTupleList)

    # print(restMatrixRDD.take(1))
    # print(restTupleListRDD.count())

    restTupleList = restTupleListRDD.collect()  # release later!

    restForALS = []  # release later!
    for item in restTupleList:
        restForALS.extend(item)

    restForALS_RDD = sc.parallelize(restForALS)
    trainingSetALS_RDD = restForALS_RDD.filter(lambda line: line[2] != -1.0)
    predictionSetRDD = restForALS_RDD.filter(lambda line: line[2] == -1.0)
    print(predictionSetRDD.count())
    print(trainingSetALS_RDD.take(10))

    ratingsRDD = trainingSetALS_RDD.map(lambda l: Rating(l[0], l[1], l[2]))

    print(ratingsRDD.take(10))

    # Build the recommendation model using Alternating Least Squares
    rank = 10
    numIterations = 10
    model = ALS.train(ratingsRDD, rank, numIterations)
    test = predictionSetRDD.map(lambda line: (line[0], line[1]))
    print(test.count())
    predictions = model.predictAll(test)
    predictionList = predictions.collect()
    print(type(predictionList))
    print(len(predictionList))

    # combine the test set and prediction set together
    predRestRDD = trainingSetALS_RDD.union(predictions)
    print(predRestRDD.take(10))
    predRestMatRDD = predRestRDD.map(lambda line: (line[0], (line[1], line[2]))).reduceByKey(lambda a, b: a + b)
    predRestMatRDD = predRestMatRDD.map(Restaurant.reconstructVector).filter(lambda (a, b): -1.0 not in b)
    predRestMatRDD = predRestMatRDD.map(Restaurant.removeNegValues)
    print(predRestMatRDD.take(1))
    print(predRestMatRDD.count())

    mat = RowMatrixWithSimilarity(predRestMatRDD.map(lambda (a, b): b))
    #mat.rows.saveAsTextFile("../../../data/rows")
    m = mat.numRows()  # 4
    n = mat.numCols()  # 3
    print((m, n))
    similarityEstimate = mat.columnSimilarities().entries
    if os.path.exists("../../../data/similarities"):
        os.removedirs("../../../data/similarities")
    similarityEstimate.saveAsTextFile("../../../data/similarities")
    #model = PowerIterationClustering.train(similarityEstimate, 2, 10)


    '''
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

