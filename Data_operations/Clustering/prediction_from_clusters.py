import sys
sys.path.append("Data_operations")

from Tool_Functions.cleaning_data import * 

from new_data_set_all import * 
from viualize_datas import * 
from new_data_set_all import *

from matplotlib.backends.backend_pdf import PdfPages
import pandas as pd
import matplotlib.pyplot as plt

data_path = "/Users/maximecoppa/Desktop/Statapp_Data/Datas/"

df = file_to_dataframe(data_path + "df_all_clusters.csv")

# Load dataframe

def courbe_reabo(filename = data_path + "df_all_clusters.csv"):
    
    df = file_to_dataframe(filename)
    list_promo= df["TYPE_PROMON"].unique()

    with PdfPages(data_path + 'tendances_reabo_cluster.pdf') as pdf:
        for i in range(8):
            df_filtered = df[df['Cluster_8'] == i]
            df_filtered['DATE_ACTE_REEL'] = pd.to_datetime(df_filtered['DATE_ACTE_REEL'])
            df_filtered['mois_annee'] = df_filtered['DATE_ACTE_REEL'].dt.strftime('%Y-%m')
            df_filtered = count_abo_conditions(df_filtered, ["TYPE_PROMON", 'DATE_ACTE_REEL'], 'ID_ABONNE')

            plt.figure(figsize=(10, 6))
            for el in list_promo:
                sns.lineplot(x='DATE_ACTE_REEL', y='NB_ID_ABONNE', data=df_filtered[df_filtered["TYPE_PROMON"] == el].sort_values(by='DATE_ACTE_REEL'), label=el)

            plt.title('Evolution of Reabo of the cluster ' + str(i))
            plt.xlabel('DATE_ACTE_REEL')
            plt.ylabel('NB_ID_ABONNE')
            plt.legend()

            pdf.savefig()
            plt.close()

df["DELAI_REABO_WEEK"] = df["DELAI_REABO"]//7
print(count_abo_conditions(df[df['Cluster_8'] == 0],["DELAI_REABO_WEEK","TYPE_PROMON"],"ID_ABONNE"))