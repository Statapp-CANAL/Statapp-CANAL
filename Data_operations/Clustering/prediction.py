
import sys
sys.path.append("Data_operations")

from Tool_Functions.cleaning_data import *  # Import custom data cleaning functions
from sklearn.preprocessing import StandardScaler  # Import StandardScaler for data normalization

from sklearn.cluster import KMeans  # Import KMeans clustering algorithm
from sklearn.metrics import silhouette_samples, silhouette_score  # Import silhouette metrics

from viualize_datas import * 
from new_data_set_all import * 

import matplotlib.cm as cm  # Import colormap for visualizations
import matplotlib.pyplot as plt  # Import plotting library
import numpy as np  

data_path = ""

def clean_new_df(data_path):
    df = file_to_dataframe(data_path + 'New_abos.csv')
    df = df.rename( columns= {'DAT_MVT_REELLE' : 'DATE_ACTE_REEL'})
    df['DATE_ACTE_REEL'] = pd.to_datetime(df['DATE_ACTE_REEL'])
    save_to_csv_file(df, data_path + 'new_abos.csv')
    return True

clean_new_df(data_path)

def upload_new_df(data_path):
    df_original = file_to_dataframe(data_path + "df_Donnees_Reabos_odd_v2.csv")
    dfn = file_to_dataframe(data_path + 'New_abos.csv')
    df_test = join_dataFrames_outer(df_original,dfn, 'ID_ABONNE')
    df_test['DATE_ACTE_REEL_x'].fillna(df_test['DATE_ACTE_REEL_y'], inplace=True)
    df_test.drop(columns=['DATE_ACTE_REEL_y'], inplace=True)
    df_test = df_test.rename( columns= {'DATE_ACTE_REEL_x' : 'DATE_ACTE_REEL'})
    save_to_csv_file(df_test, data_path + 'all_reabos_nov.csv')
    return True

upload_new_df(data_path)

def upload_new_df_clusters_partiel(data_path):
    df_test = file_to_dataframe(data_path + 'all_reabos_nov.csv')
    df_id = file_to_dataframe(data_path + 'clusters_id_partiel.csv')
    df_merge = pd.merge(df_test, df_id, on='ID_ABONNE', how='left')
    df_merge['Cluster_8'].fillna('unclustered', inplace=True)
    save_to_csv_file(df_merge, data_path + 'all_reabos_nov_n_clusters.csv')
    return True

upload_new_df_clusters_partiel(data_path)


def upload_new_df_clusters_total(data_path):
    df_test = file_to_dataframe(data_path + 'all_reabos_nov.csv')
    df_id = file_to_dataframe(data_path + 'clusters_id_all.csv')
    df_merge = pd.merge(df_test, df_id, on='ID_ABONNE', how='left')
    df_merge['KMEANS'].fillna('unclustered', inplace=True)
    save_to_csv_file(df_merge, data_path + 'all_reabos_nov_n_clusters_total.csv')
    return True

upload_new_df_clusters_total(data_path)