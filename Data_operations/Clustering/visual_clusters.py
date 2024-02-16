from algo_Kmeans import *
from algo_svm import *

#centroids = np.asarray([[0, 1, 0],[1, 0, 1], [2, 2, 2]])
centroids = np.asarray([[0, 0, 0],[1, 1, 1], [2, 2, 2]])
# In the factice case, we see how the selection of the centroids can distort the analysis.
# Especially, it chooses the plan in which the clusters will appear.

features = ['NB_ODD_15_TC', 'NB_ODD_15_EV', 'NB_ODD_30_TC']
df = 'dataframe_factice.csv'
path = "/Users/clementgadeau/Statapp/Clustering/"

data_frame_kmeans = algo_kmeans(3, centroids = centroids, features = features, df = df, path = path)
data_frame_svm = algo_svm(nu = 0.9, kernel='rbf', gamma = 0.1, features = features, df = df, path = path)

#tracer_kmeans(data_frame_kmeans, features, centroids = 'k-means++')
tracer_svm(data_frame_svm, features)
