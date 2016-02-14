from __future__ import print_function
from pyspark.mllib.linalg.distributed import RowMatrix
from pyspark.mllib.linalg.distributed import CoordinateMatrix
from pyspark import SparkContext


class RowMatrixWithSimilarity(RowMatrix):
    def columnSimilarities(self):
        """
        Compute all cosine similarities between columns.

        >>> rows = sc.parallelize([IndexedRow(0, [1, 2, 3]),
        ...                        IndexedRow(6, [4, 5, 6])])
        >>> mat = IndexedRowMatrix(rows)
        >>> cs = mat.columnSimilarities()
        >>> print(cs.numCols())
        3
        """
        java_coordinate_matrix = self._java_matrix_wrapper.call("columnSimilarities")
        return CoordinateMatrix(java_coordinate_matrix)


def _test():
    sc = SparkContext('local')
    # Create an RDD of vectors.
    rows = sc.parallelize([[1, 2, 3], [4, 5, 6], [7, 8, 9], [10, 11, 12]])

    # Create a RowMatrix from an RDD of vectors.
    mat = RowMatrixWithSimilarity(rows)

    # Get its size.
    m = mat.numRows()  # 4
    n = mat.numCols()  # 3

    # Get the rows as an RDD of vectors again.
    rowsRDD = mat.rows

    simsEstimate = mat.columnSimilarities()
    print(simsEstimate.entries.collect())
    print(m, n)


if __name__ == "__main__":
    _test()
