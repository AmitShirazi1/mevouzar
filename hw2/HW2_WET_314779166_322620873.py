
from pyspark.ml.feature import VectorAssembler
import pyspark.sql.functions as F
from pyspark.ml.linalg import Vectors
import numpy as np
import sys
from pyspark.sql import types as T
from pyspark.ml.stat import Summarizer
from pyspark.sql import Row

# "Automating" the creation of vector out of a dataframe (that has the same schema as the 'data_df' dataframe)
col_names = [field.name for field in data_df.schema.fields]
assembler = VectorAssembler(inputCols=col_names, outputCol="point")

# Calculate distances between a given data point and each centroid, return the closest centroid's label.
def update_label(point, centroids):
  min_dist = sys.float_info.max
  for j in range(len(centroids)):
    # We don't have to sqrt the distances because the comparison relation still stays the same,
    # it's still right to compare between the sqared distances - יחס סדר נשמר כעושים שורש (על מספרים אי שליליים)
    dist = float(Vectors.squared_distance(point, centroids[j]))

    # Finding the closest centroid and updating the point's label to the centroid's corresponding label.
    if min_dist > dist:
      min_dist = dist
      label = j
  return int(label)

# Calculating the new centroid by the points assigned to its corresponding label according to average.
def update_centroid(data):
  new_centroid = []
  i = 0
  # Calculating the average of the points by calculating it separately on each vector component
  # and than combining them into a list according to the order of the components.
  for col in col_names:
    i += 1
    avg = data.select(col).agg(F.avg(data[col])).first()[0]
    new_centroid.append(avg)
  # Converting the centroid from type list to a vector.
  centroid_df = spark.createDataFrame([new_centroid], schema=col_names)
  return assembler.transform(centroid_df).select("point").first()[0]


def kmeans_fit(data: pyspark.sql.DataFrame,
               init: pyspark.sql.DataFrame,
               k: int = 4,
               max_iter: int = 10):
  """
  Inputs:
    data - a PySpark DataFrame that includes the data, as given to you from Moodle
    init - a PySpark DataFrame that holds the intial k centroids to be used in the algorithm
    k - an integer - the amount of clusters in the algorithm
    max_iter - an integer - the maximum amount of iterations before terminating the algorithm

  Outputs:
    returns - centroids - a PySpark DataFrame that contains the final centroids from the algorithm
  """

  # Adding a column named 'point' that will contain the values of all columns as a vector in each row.
  data = assembler.transform(data_df).cache()
  data_df.unpersist()
  # Creating a list that contains all the centroids as vectors.
  centroids_list = assembler.transform(init_centroids).select("point").rdd.map(lambda x: x[0]).collect()

  update_label_udf = F.udf(lambda point: update_label(point, centroids_list), T.IntegerType())
  # Assigning two column so that one will contain the new updated labels and will contain the old ones.
  # In each iteration (of updating labels) each column is being updated alternatively so we could save the old labels easily.
  label_cols = ['label1', 'label2']
  labels = len(centroids_list)

  # Loop until we're exceeding the maximum number of iterations.
  for i in range(max_iter):
    j = i % 2
    # Updating the label of each point in the data.
    data = data.withColumn(label_cols[j], update_label_udf("point")).cache()
    # Check if the clusters have reached convergence (i.e., the assignments no longer changes)
    if i > 0:
      count_label_changes = data.filter(data['label1'] != data['label2'])\
                                .count()
      if count_label_changes == 0:
        break

    temp_list = []
    # Re-calculating each cluster's centroid.
    for label in range(labels):
      cent_vec = update_centroid(data.filter(F.col(label_cols[j]) == label))
      temp_list.append(cent_vec)

    data.unpersist()
    # Updating the list of centroids to the newly calculated ones.
    centroids_list = temp_list

  # Converting the list of centroids to a dataframe, and returning it.
  return sc.parallelize([Row(centroids=centroid) for centroid in centroids_list]).toDF()
