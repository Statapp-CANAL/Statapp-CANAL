import pandas as pd
import numpy as np


def file_to_dataframe(filename, st=","):
    """
    Initialize the DataFrame from a filenames
    Parameters:
    -----------
    filenames: str, the name of the file
    st: str, helps to delimites the columns of the datas because "Correspondances_promo
    are delimited with ';' while the other are delimited with ','.
    """
    datas = pd.read_csv(filename, delimiter=st)
    return datas


def join_dataFrames(df1, df2, cond):
    """
    Join two DataFrames on the conditions cond
    Parameters:
    -----------
    df1, df2: DataFrames
    cond: str or list of the columns on which we want to join
    """
    df_join = pd.merge(df1, df2, on=cond, how="inner")

    undesired_columns = [
        col for col in df_join.columns if col.startswith("Unnamed")
    ]  # test if there are any columns which are created and are not useful
    if undesired_columns:
        df_join = df_join.drop(columns=undesired_columns)  # drop unuseful columns

    return df_join


def join_dataFrames_outer(df1, df2, cond):
    """
    Join two DataFrames on the conditions cond
    Parameters:
    -----------
    df1,df2: DataFrames
    cond: str list of the colomuns on which we want to join
    """
    df_join = pd.merge(df1, df2, on=cond, how="outer")

    undesired_columns = [
        col for col in df_join.columns if col.startswith("Unnamed")
    ]  # test if there are any columns which are created and are not useful
    if undesired_columns:
        df_join = df_join.drop(columns=undesired_columns)  # drop unuseful columns

    return df_join


def calculate_nb_promos(df):
    return df.groupby("CPROMO")["ID_ABONNE"].count()
