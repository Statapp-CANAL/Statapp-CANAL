# Importation des bibliothèques nécessaires
import sys
sys.path.append("Data_operations")
from Tool_Functions.graphs_clusters import *

import pandas as pd
import numpy as np
from mpl_toolkits.mplot3d import Axes3D
from sklearn.cluster import KMeans
import matplotlib.pyplot as plt
import itertools



def algo_kmeans(nb_clusters, centroids = 'k-means++', features = ['NB_ODD_15_TC', 'NB_ODD_15_EV', 'NB_ODD_30_TC'], df = 'dataframe_factice.csv', path = "/Users/clementgadeau/Statapp/Clustering/"):
    """
    class sklearn.cluster.KMeans(n_clusters=8, init='k-means++', n_init=10, max_iter=300, tol=0.0001, 
    precompute_distances='auto', verbose=0, random_state=None, copy_x=True, n_jobs=None, algorithm='auto')
    
    init = This is where you can set the initial cluster centroids
    n_init = By default is 10 and so the algorithm will initialize the centroids 10 times and will pick the most converging value as the best fit. Increase this value to scan the entire feature space.
    tol = If we set this to a higher value, then it implies that we are willing to tolerate a larger change in inertia, or change of loss, before we declare convergence (sort of like how fast are we converging). So if the change of inertia is less than the value specified by tol, then the algorithm will stop iterating and declare convergence even if it has completed fewer than max_iter rounds. Keep it at a low value to scan the entire feature space.
    Bien normaliser les données !
    """
    data = pd.read_csv(path + df)
    # Sélectionner les colonnes à utiliser pour le clustering
    X = data[features]  

    kmeans = KMeans(nb_clusters, init = centroids)
    kmeans.fit(X)

    # Ajouter les étiquettes des clusters à notre DataFrame
    data['Cluster'] = kmeans.labels_
    print(data.head())
    data.to_csv(path + 'dataframe_clustured_by_Kmeans.csv')
    # Afficher les centres des clusters
    print(kmeans.cluster_centers_)

    return data


def tracer_kmeans(data_frame, axes, centroids = 'k-means++'):
    graphs_3D(axes, data = data_frame, model = 'kmeans', kmeans = None, centroids = False)

    compteur = 1
    print(axes[:-1])
    for axe1 in axes[:-1]:
        for axe2 in axes[compteur:]:
            graphs_2D(axes = [axe1, axe2], data = data_frame, model = 'kmeans', kmeans = None, centroids = False)
        compteur += 1

    return
