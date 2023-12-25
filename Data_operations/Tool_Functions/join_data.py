import pandas as pd
import numpy as np


def file_to_dataframe(filename,st): 
    """
    Initialize the DataFrame from a filenames 
    Parameters: 
    -----------
    filenames: str, the name of the file
    st: str, helps to delimites the columns of the datas because "Correspondances_promo
    are delimited with ';' while the other are delimited with ','.
    """
    datas = pd.read_csv(filename,delimiter=st)
    return datas

def join_dataFrames(df1,df2,cond):
    """
    Join two DataFrames on the conditions cond
    Parameters: 
    -----------
    df1,df2: DataFrames
    cond: str list of the colomuns on which we want to join 
    """
    return pd.merge(df1, df2, on=cond, how='inner')

def calculate_nb_promos(df):
    return(df.groupby('CPROMO')['ID_ABONNE'].count())

data_path_head = "/Users/maximecoppa/Desktop/Statapp/Datas_head/"
data_path_clean = "/Users/maximecoppa/Desktop/Statapp/Datas_clean/"


df_Correspondances_Promos = file_to_dataframe(data_path_head + "df_Correspondances_Promos.csv",",")
df_Données_Promos_2021 = file_to_dataframe(data_path_clean + "df_Données_Promos_2021.csv",",")
df_Données_Reabos_2021 = file_to_dataframe(data_path_clean + "df_Données_Reabos_2021.csv",",")

df_join = join_dataFrames(df_Données_Promos_2021,df_Données_Reabos_2021,['ID_ABONNE','DATE_ACTE_REEL'])
n,_ = df_join.shape
df_nb_promos = calculate_nb_promos(df_join)
print((df_nb_promos).sort_values(ascending=False))
print(df_Données_Promos_2021.shape)
print(df_Données_Reabos_2021.shape)