from __future__ import print_function

from pyspark.mllib.clustering import KMeans, KMeansModel
from numpy import array
from math import sqrt

from pyspark import SparkContext


# Evaluate clustering by computing Within Set Sum of Squared Errors
def error(point):
    center = clusters.centers[clusters.predict(point)]
    return sqrt(sum([x**2 for x in (point - center)]))

if __name__ == "__main__":
    # Load and parse the data
    sc = SparkContext('local')
    data = sc.textFile("../../../data/rows.txt")
    parsedData = data.map(lambda line: array([float(x) for x in line.strip().strip("[]").split(',')]))
    #for i in xrange(20):
    # Build the model (cluster the data)
    clusters = KMeans.train(parsedData, 4096, maxIterations=10, initializationMode="random")
    WSSSE = parsedData.map(lambda point: error(point)).reduce(lambda x, y: x + y)
    print("Within Set Sum of Squared Error = " + str(WSSSE))

    # Save and load model
    '''
    clusters.save(sc, "myModelPath")
    sameModel = KMeansModel.load(sc, "myModelPath")
    '''
