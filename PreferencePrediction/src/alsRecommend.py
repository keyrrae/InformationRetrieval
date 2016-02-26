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
from matplotlib import pyplot as plt


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


def parseReviewToTrainingSet(reviewRDD, testReviewRDD):
    reviewRestaRDD = reviewRDD.map(Review.toString).filter(lambda line: Review.is_res(line, restaurantListBC))
    userList = reviewRestaRDD.map(Review.getuserid).sortByKey().collect()
    restList = reviewRestaRDD.map(Review.getbusiid).sortByKey().collect()
    userListBC = sc.broadcast(userList)
    restListBC = sc.broadcast(restList)

    print(userList[10])

    '''
    Generate Dictionaries of users in training set and broadcast
    '''
    userIdToNumDict, userNumToIdDict = assignNum(userList)

    userIdToNumDictBC = sc.broadcast(userIdToNumDict)
    userNumToIdDictBC = sc.broadcast(userNumToIdDict)

    '''
    Generate Dictionaries of Restaurants in training set and broadcast
    '''
    restIdToNumDict, restNumToIdDict = assignNum(restList)

    restIdToNumDictBC = sc.broadcast(restIdToNumDict)
    restNumToIdDictBC = sc.broadcast(restNumToIdDict)


    userReviewRestaRDD = reviewRestaRDD.map(Review.mapper).reduceByKey(Review.reducer).map(Review.reshape)
    userReviewRestaCollNormRDD = userReviewRestaRDD.map(Review.normalize)  # Subtract average values
    userReviewRestaNormLst = userReviewRestaCollNormRDD.collect()  # map(Review.flatten).flatMap(Review.vectorize)
    userAvgDict = dict(userReviewRestaCollNormRDD.map(lambda x: (x[0], x[1])).collect())

    userReviewRestaLst = parseUserBusiLst(userReviewRestaNormLst, userIdToNumDict, restIdToNumDict)

    userReviewRestaNormRDD = sc.parallelize(userReviewRestaLst)
    #usrResStarTupleRDD = reviewRestaRDD.map(Review.getUsrResStar)


    testReviewRestRDD = testReviewRDD.map(Review.toString).map(Review.getUsrResStar)

    print(userReviewRestaNormRDD.take(10))
    testReviewRestRDD = testReviewRestRDD.filter(lambda x: x[0] in userListBC.value and x[1] in restListBC.value)\
        .map(lambda x: Review.normalizeStar(userAvgDict, x))\
        .map(lambda x: Review.replaceIDwithNum(x, userIdToNumDictBC, restIdToNumDictBC))
        #.filter(lambda x: x[0] != 0 and x[1] != 0)
    print(testReviewRestRDD.take(10))
 # and x[1] in restaurantListBC.value)\
    return userReviewRestaNormRDD, testReviewRestRDD

if __name__ == "__main__":
    sc = SparkContext('local')

    businessRDD = sc.textFile("../../../data/yelp_academic_dataset_business.json")
    sc.setCheckpointDir("checkpoints/")

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

    reviewTestRDD = sc.textFile("../../../data/yelp_academic_dataset_review_tail.json")

    userReviewRestaNormRDD, testReviewRestRDD = parseReviewToTrainingSet(reviewRDD, reviewTestRDD)
    print(userReviewRestaNormRDD.take(10))
    print("============")
    print(testReviewRestRDD.take(10))

    # Build the recommendation model using Alternating Least Squares
'''
    ranks = range(2, 10) #21
    lambdas = [0.005, 0.01, 0.015, 0.02, 0.025, 0.03]
    symbols = ['*-', 'x-', 'o-', '^-', 'D-', '+-', 's-', 'v-', '<-', '>-']
    numIters = [5, 10, 20]
    bestModel = None
    bestValidationRmse = float("inf")
    bestRank = 0
    bestLambda = -1.0
    bestNumIter = -1
    testdata = testReviewRestRDD.map(lambda p: (p[0], p[1]))


    rmseVsRanks = []
    #for rank, lmbda, numIter in itertools.product(ranks, lambdas, numIters):
    fixNumIter = 20
    for lmbda in lambdas:
        ALS.checkpointInterval = 2
        rmseLst = []
        for rank in ranks:
            model = ALS.train(userReviewRestaNormRDD, rank, fixNumIter, lmbda)
            predictions = model.predictAll(testdata).map(lambda r: ((r[0], r[1]), r[2]))
            ratesAndPreds = userReviewRestaNormRDD.map(lambda r: ((r[0], r[1]), r[2])).join(predictions)
            validationRmse = math.sqrt(ratesAndPreds.map(lambda r: (r[1][0] - r[1][1])**2).mean())
            rmseLst.append(validationRmse)
        rmseVsRanks.append(rmseLst)
    print(len(rmseVsRanks[0]))

    plt.figure(1)
    for i, line in enumerate(rmseVsRanks):
        plt.plot(ranks, line, symbols[i])
    plt.show()
'''

'''
    lamb = 0.01
    rank = 8
    numIterations = 20
    model = ALS.train(userReviewRestaNormRDD, rank, numIterations, lamb)

    # Evaluate the model on training data
    testdata = userReviewRestaNormRDD.map(lambda p: (p[0], p[1]))
    predictions = model.predictAll(testdata).map(lambda r: ((r[0], r[1]), r[2]))
    ratesAndPreds = userReviewRestaNormRDD.map(lambda r: ((r[0], r[1]), r[2])).join(predictions)
    RMSE = math.sqrt(ratesAndPreds.map(lambda r: (r[1][0] - r[1][1])**2).mean())
    print(ratesAndPreds.collect())
    print("Mean Squared Error = " + str(RMSE))
    #for rank, lmbda, numIter in itertools.product(ranks, lambdas, numIters):

    for rank in ranks:
        ALS.checkpointInterval = 2
        rankLst = []
        model = ALS.train(userReviewRestaNormRDD, rank, numIter, lmbda)
        predictions = model.predictAll(testdata).map(lambda r: ((r[0], r[1]), r[2]))
        ratesAndPreds = userReviewRestaNormRDD.map(lambda r: ((r[0], r[1]), r[2])).join(predictions)
        validationRmse = math.sqrt(ratesAndPreds.map(lambda r: (r[1][0] - r[1][1])**2).mean())

        print("RMSE (validation) = %f for the model trained with " % validationRmse + \
              "rank = %d, lambda = %f, and numIter = %d." % (rank, lmbda, numIter))
        res.append((rank, lmbda, numIter, validationRmse))
        if (validationRmse < bestValidationRmse):
            bestModel = model
            bestValidationRmse = validationRmse
            bestRank = rank
            bestLambda = lmbda
            bestNumIter = numIter

    print("The best model was trained with rank = %d and lambda = %.2f, " % (bestRank, bestLambda) \
      + "and numIter = %d, and its RMSE on the validation set is %f." % (bestNumIter, bestValidationRmse))

    print(res)
    '''

   # userRDD = sc.textFile("../../../data/yelp_academic_dataset_user.json")
   # userRDD = userRDD.map(User.toString).map(User.getFriends)  # user friendlist


