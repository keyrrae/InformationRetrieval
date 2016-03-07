from __future__ import print_function
from busiMR import Business
from busiMR import Restaurant
from busiMR import attributeAccumulatorParam
from reviewMR import Review
from userMR import User

import os
import itertools
import math
import sys
from operator import add

from pyspark import SparkConf, SparkContext
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

def printUsage():
    print("alsRecommend.py [-e/-c/-d/-cr/-dr] [numOfReviews/MyRatingFileName]")
    print("Parameters:")
    print("-e\tevaluate models")
    print("-c\tlet the program choose the best model")
    print("-d\tuse the default model (NumOfIter = 25, NumOfFeature = 25, lambda = 10.0)")
    print("-cr\tlet the program choose the best model, and recommend restaurants for you")
    print("-dr\tuse the default model, and recommend restaurants for you")

def computeRmse(model, evalSet):
    evalSetUserProduct = evalSet.map(lambda x: (x[0], x[1]))
    predictions = model.predictAll(evalSetUserProduct).map(lambda r: ((r[0], r[1]), r[2]))
    ratesAndPreds = evalSet.map(lambda r: ((r[0], r[1]), r[2])).join(predictions)
    validationRmse = math.sqrt(ratesAndPreds.map(lambda r: (r[1][0] - r[1][1])**2).mean())
    return validationRmse


if __name__ == "__main__":
    conf = SparkConf() \
      .setAppName("YelpReviewALS") \
      .set("spark.executor.memory", "1g")
    sc = SparkContext('local', conf=conf)

    reviewRDD = sc.textFile("../../../data/review_large.txt")
    sc.setCheckpointDir("checkpoints/")

    if len(sys.argv) < 2:
        printUsage()
        exit(1)

    if sys.argv[1] == '-e':
        evalModel = True
    else:
        evalModel = False

    if sys.argv[1] == '-c':
        runValidation = True
    else:
        runValidation = False

    if sys.argv[1] == '-cr':
        runValidation = True
        runRecommend = True
    else:
        runValidation = False
        runRecommend = False

    if sys.argv[1] == '-dr':
        runRecommend = True
    else:
        runRecommend = False

    defaultNumOfReviews = 1600000

    if len(sys.argv) == 3:
        if runRecommend:
            myRatings = sc.textFile(sys.argv[2])
            myRatings = myRatings.map(lambda x: str(x).strip().split(','))\
                        .map(lambda x: (0, ("000000000000000000000", x[0], int(x[1]))))
            print(myRatings.first())
            numOfReviews = defaultNumOfReviews
        else :
            numOfReviews = int(sys.argv[2])
            if numOfReviews > 1300000:
                print("Sorry, we don't have that big training set.")
                exit(1)
    else:
        numOfReviews = defaultNumOfReviews


    ratings = sc.parallelize(reviewRDD.take(numOfReviews)).map(lambda x: str(x).strip().split(','))\
                .map(lambda x: (int(x[0]), (x[1], x[2], int(x[3]))))

    if runRecommend:
        ratings = ratings.union(myRatings)

    numRatings = ratings.count()
    numUsers = ratings.values().map(lambda r: r[0]).distinct().count()
    numRestaurants = ratings.values().map(lambda r: r[1]).distinct().count()

    print("Got %d ratings from %d users on %d restaurants." % (numRatings, numUsers, numRestaurants))

    userList = ratings.values().map(lambda r: r[0]).distinct().collect()
    restList = ratings.values().map(lambda r: r[1]).distinct().collect()

    getUserIndex, getUserID = assignNum(userList)
    getRestIndex, getRestID = assignNum(restList)

    getUserIDBC = sc.broadcast(getUserID)
    getUserIndexBC = sc.broadcast(getUserIndex)
    getRestIndexBC = sc.broadcast(getRestIndex)
    getRestIDBC = sc.broadcast(getRestID)

    ratings = ratings.map(lambda (x, y): (x, Review.replaceIDwithNum(y, getUserIndexBC, getRestIndexBC)))
    usrRatingAvg = ratings.values().map(lambda x: (x[0], x[2])).reduceByKey(Review.reducer).map(Review.reshape)\
                    .filter(lambda x: len(x[1]) >= 3).map(Review.reshapeList)\
                    .map(lambda x: (x[0], sum(x[1])/float(len(x[1]))))
    usrRatingAvgBC = sc.broadcast(dict(usrRatingAvg.collect()))
    ratings = ratings.filter(lambda x: x[1][0] in usrRatingAvgBC.value).map(lambda (x, y): (x, Review.subtractAvg(y, usrRatingAvgBC)))

    numOfPartitions = 4

    trainingVal = ratings.filter(lambda x: x[0] <= 6) \
        .values()

    trainingMean = trainingVal.map(lambda x: x[2]).mean()

    training = trainingVal.repartition(numOfPartitions).cache()

    trainingUsrSet = set(trainingVal.map(lambda x: x[0]).collect())

    trainingRestSet = set(trainingVal.map(lambda x: x[1]).collect())

    trainingUsrSetBC = sc.broadcast(trainingUsrSet)
    trainingRestSetBC = sc.broadcast(trainingRestSet)

    validation = ratings.filter(lambda x: 6 < x[0] <= 8) \
        .values() \
        .filter(lambda x: x[0] in trainingUsrSetBC.value and x[1] in trainingRestSetBC.value)\
        .repartition(numOfPartitions) \
        .cache()

    test = ratings.filter(lambda x: x[0] > 8).values()\
        .filter(lambda x: x[0] in trainingUsrSetBC.value and x[1] in trainingRestSetBC.value).cache()

    testMean = test.map(lambda x: x[2]).mean()

    test = test.map(lambda x: (x[0], x[1], x[2] - (testMean - trainingMean)))
    numTraining = training.count()
    numValidation = validation.count()
    numTest = test.count()

    print("Training: %d, validation: %d, test: %d" % (numTraining, numValidation, numTest))

    if evalModel:
        ranks = range(2, 21)
        lambdas = [0.001, 0.01, 0.1, 1.0, 10.0]
        symbols = ['*-', 'x-', 'o-', '^-', 'D-', '+-', 's-', 'v-', '<-', '>-']
        fixNumIter = 20

        rmseVsRanks = []

        for lmbda in lambdas:
            ALS.checkpointInterval = 2
            rmseLst = []
            for rank in ranks:
                model = ALS.train(training, rank, fixNumIter, lmbda)
                validationRmse = computeRmse(model, validation)
                rmseLst.append(validationRmse)
            rmseVsRanks.append(rmseLst)

        plt.figure(1)
        plt.grid(True)
        for i, line in enumerate(rmseVsRanks):
            plt.plot(ranks, line, symbols[i], label="lambda = "+str(lambdas[i]))
        plt.legend(loc='upper right', shadow=True)
        plt.xlabel("Number of Features")
        plt.ylabel("RMSE")

        rmseVsNumIter = []
        NumIter = range(2, 26)
        fixRank = 20
        for lmbda in lambdas:
            ALS.checkpointInterval = 2
            rmseLst = []
            for numIter in NumIter:
                model = ALS.train(training, fixRank, numIter, lmbda)
                validationRmse = computeRmse(model, validation)
                rmseLst.append(validationRmse)
            rmseVsNumIter.append(rmseLst)

        plt.figure(2)
        plt.grid(True)
        for i, line in enumerate(rmseVsNumIter):
            plt.plot(NumIter, line, symbols[i], label="lambda = "+str(lambdas[i]))
        plt.legend(loc='upper right', shadow=True)
        plt.xlabel("Number of Iterations")
        plt.ylabel("RMSE")
        plt.show()

    elif runValidation:
        ranks = [10, 15, 20, 25]
        lambdas = [0.001, 0.01, 1.0, 10.0]
        numIters = [10, 15, 20]
        bestModel = None
        bestValidationRmse = float("inf")
        bestRank = 0
        bestLambda = -1.0
        bestNumIter = -1

        for rank, lmbda, numIter in itertools.product(ranks, lambdas, numIters):
            model = ALS.train(training, rank, numIter, lmbda)
            validationRmse = computeRmse(model, validation)

            print("RMSE (validation) = %f for the model trained with " % validationRmse +
                  "rank = %d, lambda = %.2f, and numIter = %d." % (rank, lmbda, numIter))

            if validationRmse < bestValidationRmse:
                bestModel = model
                bestValidationRmse = validationRmse
                bestRank = rank
                bestLambda = lmbda
                bestNumIter = numIter

        testRmse = computeRmse(bestModel, test)

        # evaluate the best model on the test set
        print("The best model was trained with rank = %d and lambda = %.2f, " % (bestRank, bestLambda) +
              "and numIter = %d, and its RMSE on the test set is %f." % (bestNumIter, testRmse))

    else:
        fixRank = 25
        fixLambda = 5.0
        fixNumIter = 25

        model = ALS.train(training, fixRank, fixNumIter, fixLambda)
        bestModel = model
        testRmse = computeRmse(model, test)
        print("Model training on a data set of %d " % numTraining)
        print("rank = %d and lambda = %.2f, " % (fixRank, fixLambda) +
              "and numIter = %d, and its RMSE on the test set is %f." % (fixNumIter, testRmse))

        meanRating = training.union(validation).map(lambda x: x[2]).mean()
        baselineRmse = math.sqrt(test.map(lambda x: (meanRating - x[2]) ** 2).reduce(add) / numTest)
        improvement = (baselineRmse - testRmse) / baselineRmse * 100
        print("The best model improves the baseline by %.2f" % improvement + "%.")

    if runRecommend:
        myRatedRestIds = set([item for item in myRatings.map(lambda x: x[1][1]).collect()])
        candidates = sc.parallelize([m for m in getRestIndex.keys() if m not in myRatedRestIds])
        predictions = bestModel.predictAll(
                        candidates.map(
                            lambda x: (getUserIndexBC.value["000000000000000000000"], getRestIndexBC.value[x])))\
                      .collect()

        recommendations = sorted(predictions, key=lambda x: x[2], reverse=True)[:10]

        print("Restaurants recommended for you:")
        for i in xrange(len(recommendations)):
            print ("%2d: %s" % (i + 1, getRestID[recommendations[i][1]])) #.encode('ascii', 'ignore')


