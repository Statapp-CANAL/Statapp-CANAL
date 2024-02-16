import numpy as np
from mpl_toolkits.mplot3d import Axes3D
import matplotlib.pyplot as plt
from sklearn.cluster import KMeans


def graphs_3D(axes, data, model, kmeans = None, centroids = False):
    # Créer une figure et un axe 3D
    fig = plt.figure()
    ax = fig.add_subplot(111, projection='3d')

    # Tracer les points en 3D avec les trois premières variables
    ax.scatter(data[axes[0]], data[axes[1]], data[axes[2]], s = 5, c=data['Cluster'], cmap='viridis')

    if centroids and model == 'kmeans' :
        ax.scatter(kmeans.cluster_centers_[:, 0], kmeans.cluster_centers_[:, 1], s=30, c='red', label = 'Centroids')

    ax.set_xlabel(axes[0])
    ax.set_ylabel(axes[1])
    ax.set_zlabel(axes[2])

    plt.title('Clustering Results 3D')
    plt.legend()
    plt.show()

    return


def graphs_2D(axes, data, model, kmeans = None, centroids = False):
    # Créer une figure et un axe 3D
    fig = plt.figure()

    # Tracer les points en 3D avec les trois premières variables
    plt.scatter(data[axes[0]], data[axes[1]], s = 5, c=data['Cluster'], cmap='viridis')

    if centroids and model == 'kmeans' :
        plt.scatter(kmeans.cluster_centers_[:, 0], kmeans.cluster_centers_[:, 1], s=30, c='red', label = 'Centroids')

    plt.xlabel(axes[0])
    plt.ylabel(axes[1])

    plt.title('Clustering Results 2D')
    plt.legend()
    plt.show()

    return

