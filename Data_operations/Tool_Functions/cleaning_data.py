import pandas as pd
import numpy as np


def file_to_dataframe(filenames,st): 
    """
    Initialize the DataFrame from a filenames 
    Parameters: 
    -----------
    filenames: str, the name of the file
    st: str, helps to delimites the columns of the datas because "Correspondances_promo
    are delimited with ';' while the other are delimited with ','.
    """
    datas = pd.read_csv(filenames,delimiter=st)
    return datas


def clean_dates(df):
    """
    Clean the dates of the files with dates such as "2022-11-14T00:00:00Z" and change it into "2022-11-14"
    -----------
    df: dataframe, the dataframe that we use
    
    """
    for colu in df.columns :
        if df[colu].dtype == object: 
            df[colu] = df[colu].str.replace('T00:00:00Z', '')
    return(df)

def change_date(st):
    """
    Change a date as "28/04/2003 00:00:00.000" into one more precise "2003-04-28"
    -----------
    st: str, the date to change

    """ 
    if type(st) != str or len(st) < 10 : 
        return(st)
    else :
        date = st[:10].split("/")
        return(date[2]+"-"+date[1]+"-"+date[0])
    
def change_dates_all(df,cols):
    """
    Change the dates of some columns of a dataFrame using the function change_date
    -----------
    df: dataframe, the DataFrame on which we are working
    cols: str list, a list of columns name that we want to change

    """ 
    for colu in cols : 
        df[colu] = df[colu].apply(change_date)
    return df

def save_to_csv_file(df,filename):
    """
    Save a dataframe on a .csv file at filename
    ----------
    df: dataframe, the DataFrame that you want to save
    filename: str, the place where you want to save your data frame
    """
    df.to_csv(filename, index=False)
    return True



"""
df = file_to_dataframe("/Users/maximecoppa/Desktop/Statapp/Datas/Correspondances_Promos_2.csv",";")
df_Correspondances_Promos = change_dates_all(df,['DEBVAL', 'FINVAL', 'DEBABOMIN', 'DEBABOMAX'])
save_to_csv_file(df_Correspondances_Promos,"/Users/maximecoppa/Desktop/Statapp/Datas_clean/df_Correspondances_Promos.csv")
save_to_csv_file(df_Correspondances_Promos,"/Users/maximecoppa/Desktop/Statapp/Datas_head/df_Correspondances_Promos.csv")
df = file_to_dataframe("/Users/maximecoppa/Desktop/Statapp/Datas/Données_Promos_2021.csv",",")
df_Données_Promos_2021 = clean_dates(df)
save_to_csv_file(df_Données_Promos_2021,"/Users/maximecoppa/Desktop/Statapp/Datas_clean/df_Données_Promos_2021.csv")
save_to_csv_file(df_Données_Promos_2021.head(),"/Users/maximecoppa/Desktop/Statapp/Datas_head/df_Données_Promos_2021_head.csv")
df = file_to_dataframe("/Users/maximecoppa/Desktop/Statapp/Datas/Données_Reabos_2021.csv",",")
df_Données_Reabos_2021 = clean_dates(df)
save_to_csv_file(df_Données_Reabos_2021.head(),"/Users/maximecoppa/Desktop/Statapp/Datas_head/df_Données_Reabos_2021_head.csv")
"""



def upload_clean(data_path):
    """
    Download clean files in your data_path
    There has to be the dirty files in your data_path
    """
    df = file_to_dataframe(data_path + "Correspondances_Promos_2.csv",";")
    df_Correspondances_Promos = change_dates_all(df,['DEBVAL', 'FINVAL', 'DEBABOMIN', 'DEBABOMAX'])
    save_to_csv_file(df_Correspondances_Promos,data_path + "df_Correspondances_Promos.csv")

    for i in range(1, 4, 1):
        df = file_to_dataframe(data_path + f"Données_Promos_202{i}.csv",",")
        df_Données_Promos_202i = clean_dates(df)
        save_to_csv_file(df_Données_Promos_202i, data_path + f"df_Données_Promos_202{i}.csv")

        df = file_to_dataframe(data_path + f"Données_Reabos_202{i}.csv",",")
        df_Données_Reabos_202i = clean_dates(df)
        save_to_csv_file(df_Données_Reabos_202i,data_path + f"df_Données_Reabos_202{i}.csv")

    return

def concat_all_years(data_path):
    """
    Create new csv files that concats the 3 years at once.
    """
    for name in ["df_Données_Promos_202", "df_Données_Reabos_202"]:
        df1 = file_to_dataframe(data_path + name + "1.csv",",")
        df2 = file_to_dataframe(data_path + name + "2.csv",",")
        df3 = file_to_dataframe(data_path + name + "3.csv",",")

        df = pd.concat([df1, df2, df3], axis=0, ignore_index=True)
        print(df.head(), df.tail())
        df.to_csv(data_path + name[:-4] + ".csv", index = False)

    return
