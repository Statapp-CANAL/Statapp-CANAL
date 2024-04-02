import sys
sys.path.append("Data_operations")

from Tool_Functions.cleaning_data import *
from Tool_Functions.comportment_reabo import * 
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score
import matplotlib.pyplot as plt
import math 
import numpy as np
from sklearn import StandardScaler
 
data_path = "/Users/maximecoppa/Desktop/Statapp_Data/Datas/"
data_path_df = data_path + 'fusion_table_score_v3.csv'

df = file_to_dataframe(data_path_df)
print(df.columns)

scaler = StandardScaler()

df = df['Autres_n_REABOS', 'ODD 15 jours EV+_n_REABOS',
       'ODD 15 jours TC_n_REABOS', 'ODD 21 jours TC_n_REABOS',
       'ODD 30 jours EV+_n_REABOS', 'ODD 30 jours TC_n_REABOS',
       'ODD 7 jours autre que SG_n_REABOS', 'PAS_ODD_n_REABOS']

def normalise_column(df,name_colum,x = -3):

    df[name_colum] = df[name_colum].replace([np.inf, -np.inf], np.nan)

    column_mean = df[name_colum].mean()
    sqrt_var = np.sqrt(df[name_colum].var())

    df[name_colum] = (df[name_colum] - column_mean)/sqrt_var

    df[name_colum] = df[name_colum].replace(np.nan,x)

    return df

name_column = 'Semaine genéreuse_MEAN_TIME_DIFF'
print(df[name_column])
df = normalise_column(df,name_column)
print(df[name_column])

def normalise_data_frame(df,columns):

    for el in columns : 

        df = normalise_column(df,el)
   
    return df[columns]


def score_silouhette(df,columns,m = 2, n = 11):

    df_scaled = normalise_data_frame(df,columns)

    np.random.seed(42)
    indices = np.random.choice(range(len(df_scaled)), size=int(len(df_scaled) * 0.1), replace=False)
    sample = df_scaled.iloc[indices]
    silhouette_scores = []

    for k in range(m, n):  # Testez des valeurs de k de 2 à 10
        kmeans = KMeans(n_clusters=k, random_state=42)
        kmeans.fit(sample)
        score = silhouette_score(sample, kmeans.labels_)
        silhouette_scores.append(score)

    return silhouette_scores

silhouette_scores = score_silouhette(df,['Autres_n_REABOS', 'ODD 15 jours EV+_n_REABOS',
       'ODD 15 jours TC_n_REABOS', 'ODD 21 jours TC_n_REABOS',
       'ODD 30 jours EV+_n_REABOS', 'ODD 30 jours TC_n_REABOS',
       'ODD 7 jours autre que SG_n_REABOS', 'PAS_ODD_n_REABOS'])

plt.figure(figsize=(8, 6))
plt.plot(range(2, 11), silhouette_scores, marker='o')
plt.xlabel('Nombre de clusters')
plt.ylabel('Score de silhouette KMeans')
plt.title('Score de silhouette pour différents nombres de clusters')
plt.show()
