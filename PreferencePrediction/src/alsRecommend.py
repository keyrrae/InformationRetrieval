from __future__ import print_function
from busiMR import Business
from busiMR import Restaurant
from busiMR import attributeAccumulatorParam
from reviewMR import Review
from userMR import User

import os
import itertools
import math

from pyspark import SparkContext
from pyspark import mllib
from pyspark.mllib.linalg.distributed import RowMatrix
from pyspark.mllib.recommendation import ALS, MatrixFactorizationModel, Rating
from pyspark.mllib.clustering import PowerIterationClustering, PowerIterationClusteringModel
from myRowMatrix import RowMatrixWithSimilarity


def assignNum(lst):
    idToNumDict = {}
    numToIdDict = {}
    for i, item in enumerate(lst):
        idToNumDict[item] = i
        numToIdDict[i] = item
    return idToNumDict, numToIdDict


def parseUserBusiLst(lst, usrDict, busiDict):

    retlst = []
    for entry in lst:
        for item in entry[2]:
            #if entry[0] in usrDict and item[0] in busiDict:
            retlst.append((usrDict[entry[0]], busiDict[item[0]], item[1]))
    return retlst

#def computeRmse(model, validation, numValidation):

if __name__ == "__main__":
    sc = SparkContext('local')

    businessRDD = sc.textFile("../../../data/yelp_academic_dataset_business.json")
    sc.setCheckpointDir("checkpoints/")

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

    reviewRDD = sc.textFile("../../../data/yelp_academic_dataset_review.json")
    reviewRestaRDD = reviewRDD.map(Review.toString).filter(lambda line: Review.is_res(line, restaurantListBC))
    userListRDD = reviewRestaRDD.map(Review.getuserid).sortByKey().collect()

    idToNumDict, numToIdDict = assignNum(userListRDD)

    idToNumDictBC = sc.broadcast(idToNumDict)
    numToIdDictBC = sc.broadcast(numToIdDict)

    userReviewRestaRDD = reviewRestaRDD.map(Review.mapper).reduceByKey(Review.reducer).map(Review.reshape)
    print(userReviewRestaRDD.take(10))
    userReviewRestaCollNormRDD = userReviewRestaRDD.map(Review.normalize)  # Subtract average values
    userReviewRestaNormLst = userReviewRestaCollNormRDD.collect()  # map(Review.flatten).flatMap(Review.vectorize)
    for i in range(100):
        print(userReviewRestaNormLst[i])
    userReviewRestaLst = parseUserBusiLst(userReviewRestaNormLst, idToNumDict, restGetNum)

    if userReviewRestaLst == []:
        print("duck")
    userReviewRestaNormRDD = sc.parallelize(userReviewRestaLst)
    print(userReviewRestaNormRDD.first())
    usrResStarTupleRDD = reviewRestaRDD.map(Review.getUsrResStar)




    # Build the recommendation model using Alternating Least Squares

    ranks = [8, 10, 12, 14, 16]
    lambdas = [0.01, 0.1, 1, 10.0]
    numIters = [5, 10, 20]
    bestModel = None
    bestValidationRmse = float("inf")
    bestRank = 0
    bestLambda = -1.0
    bestNumIter = -1
    testdata = userReviewRestaNormRDD.map(lambda p: (p[0], p[1]))

    for rank, lmbda, numIter in itertools.product(ranks, lambdas, numIters):
        ALS.checkpointInterval = 2
        model = ALS.train(userReviewRestaNormRDD, rank, numIter, lmbda)
        predictions = model.predictAll(testdata).map(lambda r: ((r[0], r[1]), r[2]))
        ratesAndPreds = userReviewRestaNormRDD.map(lambda r: ((r[0], r[1]), r[2])).join(predictions)
        validationRmse = math.sqrt(ratesAndPreds.map(lambda r: (r[1][0] - r[1][1])**2).mean())

        print("RMSE (validation) = %f for the model trained with " % validationRmse + \
              "rank = %d, lambda = %.1f, and numIter = %d." % (rank, lmbda, numIter))
        if (validationRmse < bestValidationRmse):
            bestModel = model
            bestValidationRmse = validationRmse
            bestRank = rank
            bestLambda = lmbda
            bestNumIter = numIter

    print("The best model was trained with rank = %d and lambda = %.1f, " % (bestRank, bestLambda) \
      + "and numIter = %d, and its RMSE on the validation set is %f." % (bestNumIter, bestValidationRmse))

    '''
    rank = 16
    numIterations = 10
    model = ALS.train(userReviewRestaNormRDD, rank, numIterations)

    # Evaluate the model on training data
    testdata = userReviewRestaNormRDD.map(lambda p: (p[0], p[1]))
    predictions = model.predictAll(testdata).map(lambda r: ((r[0], r[1]), r[2]))
    ratesAndPreds = userReviewRestaNormRDD.map(lambda r: ((r[0], r[1]), r[2])).join(predictions)
    MSE = ratesAndPreds.map(lambda r: (r[1][0] - r[1][1])**2).mean()
    print(ratesAndPreds.collect())
    print("Mean Squared Error = " + str(MSE))
    '''

    userRDD = sc.textFile("../../../data/yelp_academic_dataset_user.json")
    userRDD = userRDD.map(User.toString).map(User.getFriends)  # user friendlist

