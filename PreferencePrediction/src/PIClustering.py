from __future__ import print_function

from pyspark.mllib.clustering import KMeans, KMeansModel
from numpy import array
from math import sqrt
from pyspark.mllib.clustering import PowerIterationClustering, PowerIterationClusteringModel


from pyspark import SparkContext


if __name__ == "__main__":
    # Load and parse the data
    sc = SparkContext('local')
    data = sc.textFile("../../../data/similarities")
    similarities = data.map(lambda line: tuple([float(x) for x in line.strip().strip("MatrixEntry(").strip(')').split(', ')]))

    print(similarities.take(1))
    # Cluster the data into two classes using PowerIterationClustering
    model = PowerIterationClustering.train(similarities, 2, 10)

    model.assignments().foreach(lambda x: print(str(x.id) + " -> " + str(x.cluster)))

    # Save and load model
    model.save(sc, "myModelPath")
    sameModel = PowerIterationClusteringModel.load(sc, "myModelPath")
