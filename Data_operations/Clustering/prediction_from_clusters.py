import sys

sys.path.append("data_operations")

from tool_functions.cleaning_data import *

from new_data_set_all import *
from viualize_datas import *
from new_data_set_all import *

from matplotlib.backends.backend_pdf import PdfPages
import pandas as pd
import matplotlib.pyplot as plt

data_path = ""  # Specify the Path of Subscribers

df = file_to_dataframe(data_path + "df_all_clusters.csv")

# Load dataframe


def courbe_reabo(filename=data_path + "df_all_clusters.csv"):

    df = file_to_dataframe(filename)
    list_promo = df["TYPE_PROMON"].unique()

    with PdfPages(data_path + "tendances_reabo_cluster.pdf") as pdf:
        for i in range(8):
            df_filtered = df[df["Cluster_8"] == i]
            df_filtered["DATE_ACTE_REEL"] = pd.to_datetime(
                df_filtered["DATE_ACTE_REEL"]
            )
            df_filtered["mois_annee"] = df_filtered["DATE_ACTE_REEL"].dt.strftime(
                "%Y-%m"
            )
            df_filtered = count_abo_conditions(
                df_filtered, ["TYPE_PROMON", "DATE_ACTE_REEL"], "ID_ABONNE"
            )

            plt.figure(figsize=(10, 6))
            for el in list_promo:
                sns.lineplot(
                    x="DATE_ACTE_REEL",
                    y="NB_ID_ABONNE",
                    data=df_filtered[df_filtered["TYPE_PROMON"] == el].sort_values(
                        by="DATE_ACTE_REEL"
                    ),
                    label=el,
                )

            plt.title("Evolution of Reabo of the cluster " + str(i))
            plt.xlabel("DATE_ACTE_REEL")
            plt.ylabel("NB_ID_ABONNE")
            plt.legend()

            pdf.savefig()
            plt.close()


df["DELAI_REABO_WEEK"] = df["DELAI_REABO"] // 7


def regrouper_delai(delai):
    if delai >= 2:
        return 2
    elif delai <= -2:
        return -2
    else:
        return delai


"""
df['DELAI_REABO_WEEK_GROUP'] = df['DELAI_REABO_WEEK'].apply(regrouper_delai)

print(df.columns)
print(df['DELAI_REABO_WEEK_GROUP'])

print(count_abo_conditions(df[df['Cluster_8'] == 5],["DELAI_REABO_WEEK_GROUP","TYPE_PROMON"],"ID_ABONNE"))

df_filtered = df[df['Cluster_8'] == 2]
df_filtered['DATE_ACTE_REEL'] = pd.to_datetime(df_filtered['DATE_ACTE_REEL'])
df_filtered['mois_annee'] = df_filtered['DATE_ACTE_REEL'].dt.strftime('%Y-%m')
df_filtered = count_abo_conditions(df_filtered, [ 'DELAI_REABO_WEEK_GROUP', 'TYPE_PROMON','DATE_ACTE_REEL'], 'ID_ABONNE')
list_delais= df["DELAI_REABO_WEEK_GROUP"].unique()
list_promos = df["TYPE_PROMON"].unique()
print(list_promos)

df_filtered = df_filtered[df_filtered["TYPE_PROMON"] == "ODD 15 jours TC"]
plt.figure(figsize=(10, 6))
for el in list_delais:
    sns.lineplot(x='DATE_ACTE_REEL', y='NB_ID_ABONNE', data=df_filtered[df_filtered["DELAI_REABO_WEEK_GROUP"] == el].sort_values(by='DATE_ACTE_REEL'), label=el)
    plt.title('Evolution of Reabo of the cluster ' + str(0))
    plt.xlabel('DATE_ACTE_REEL')
    plt.ylabel('NB_ID_ABONNE')
    plt.legend()

        
plt.show()
"""


def courbe_reabo_new(filename=data_path + "df_all_clusters.csv"):

    df = file_to_dataframe(filename)
    list_promo = df["TYPE_PROMON"].unique()
    df["DELAI_REABO_WEEK"] = df["DELAI_REABO"] // 7
    df["DELAI_REABO_WEEK_GROUP"] = df["DELAI_REABO_WEEK"].apply(regrouper_delai)

    with PdfPages(data_path + "tendances_reabo_cluster_by_promo.pdf") as pdf:
        for i in range(8):
            df_filtered = df[df["Cluster_8"] == i]
            for el in list_promo:

                df_filtered_delai_reabo = df_filtered[df_filtered["TYPE_PROMON"] == el]
                df_filtered_delai_reabo["DATE_ACTE_REEL"] = pd.to_datetime(
                    df_filtered_delai_reabo["DATE_ACTE_REEL"]
                )
                df_filtered_delai_reabo["mois_annee"] = df_filtered_delai_reabo[
                    "DATE_ACTE_REEL"
                ].dt.strftime("%Y-%m")
                df_filtered_delai_reabo = count_abo_conditions(
                    df_filtered_delai_reabo,
                    ["DELAI_REABO_WEEK_GROUP", "DATE_ACTE_REEL"],
                    "ID_ABONNE",
                )
                list_delais = df_filtered_delai_reabo["DELAI_REABO_WEEK_GROUP"].unique()
                plt.figure(figsize=(10, 6))
                for el_delais in list_delais:
                    sns.lineplot(
                        x="DATE_ACTE_REEL",
                        y="NB_ID_ABONNE",
                        data=df_filtered_delai_reabo[
                            df_filtered_delai_reabo["DELAI_REABO_WEEK_GROUP"]
                            == el_delais
                        ].sort_values(by="DATE_ACTE_REEL"),
                        label=el_delais,
                    )

                plt.title(
                    "Evolution of Reabo of the cluster "
                    + str(i)
                    + " by time of reabo using the promo "
                    + el
                )
                plt.xlabel("DATE_ACTE_REEL")
                plt.ylabel("NB_ID_ABONNE")
                plt.legend()

                pdf.savefig()
                plt.close()


courbe_reabo_new()
