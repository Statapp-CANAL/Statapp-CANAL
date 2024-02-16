# Importation des bibliothèques nécessaires
import sys
sys.path.append("Data_operations")
from Tool_Functions.graphs_clusters import *

import pandas as pd
from sklearn import svm
from sklearn.datasets import make_blobs
import matplotlib.pyplot as plt


# Générer des données synthétiques pour l'exemple
X, _ = make_blobs(n_samples=300, centers=4, cluster_std=0.60, random_state=0)


def algo_svm(nu = 0.1, kernel='rbf', gamma = 0.1, features = ['NB_ODD_15_TC', 'NB_ODD_15_EV', 'NB_ODD_30_TC'], df = 'dataframe_factice.csv', path = "/Users/clementgadeau/Statapp/Clustering/"):
    """
    C: This is the regularization parameter, which controls the penalty for incorrect classification of training points. A higher value of C will allow the model to fit the training data better, but this may lead to over-fitting. A lower value of C will allow the model to generalize better, but this may lead to underfitting.
    kernel: The kernel to be used in the SVM model. The most commonly used kernels are "linear", "poly", "rbf" (Gaussian) and "sigmoid". Each kernel has its own specific parameters.
    gamma: This parameter is used for the "rbf", "poly" and "sigmoid" kernels. It defines the influence of each formation example on the margin. A higher gamma value means a greater influence, which may lead to overfitting. A lower value of gamma means less influence and a smoother decision boundary.
    nu: This parameter is specific to OneClassSVM and controls the proportion of anomalies expected in the data. It defines the upper limit of the proportion of observations that are exceptions to the data distribution.
    degree: This parameter is used for the "poly" kernel and controls the degree of the polynomial to be used.
    coef0: This parameter is used for the "poly" and "sigmoid" kernels and controls the independence of the decision function from the input values.
    """
    df = pd.read_csv(path + df)
    
    # Sélectionner les colonnes à utiliser pour le clustering
    X = df[features]

    model = svm.OneClassSVM(nu = nu, kernel = kernel, gamma= gamma)  # Vous pouvez ajuster les hyperparamètres selon vos besoins
    model.fit(X)

    # Prédire les anomalies dans les données
    predictions = model.predict(X)

    # Ajouter les prédictions au DataFrame
    df['Cluster'] = predictions

    return df


def tracer_svm(data_frame, axes):
    graphs_3D(axes, data = data_frame, model = 'svm')

    compteur = 1
    print(axes[:-1])
    for axe1 in axes[:-1]:
        for axe2 in axes[compteur:]:
            graphs_2D(axes = [axe1, axe2], data = data_frame, model = 'svm')
        compteur += 1

    return
