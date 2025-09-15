import pandas as pd
import math
import sys

sys.path.append("data_operations")

from tool_functions.join_data import join_dataFrames


# Some functions to manipulate the dates


def str_to_date(df, col_name):
    """
    This function transforms a column of a DataFrame
    to the type date
    """
    try:
        df[col_name] = pd.to_datetime(df[col_name])
    except ValueError:
        print("This column is not a date")
        return ValueError
    return df


def df_mois_annee(df, name_col):
    """
    This function creates two a columns "MONTH" and "YEAR" of a DataFrame
    which correspond to the "MONTH" and the "YEAR" of name_col
    """
    try:
        df[name_col] = pd.to_datetime(df[name_col])
        df["MONTH"] = df[name_col].dt.month
        df["YEAR"] = df[name_col].dt.year

    except ValueError:
        print("This column is not a date")
        return ValueError

    return df


def time_reabo_columns(df, end_abo, date_reabo):
    """
    This function creates a new column in the DataFrame which represents the delay between the end
    of the last abo and  the act of reabo
    """
    df = str_to_date(df, date_reabo)
    df = str_to_date(df, end_abo)
    df["DELAI_REABO"] = (df[date_reabo] - df[end_abo]).dt.days
    return df


# 1)
# In this part we write some functions to create the new column odd


def df_filter_condition(df, str_name, str_cond):
    """
    This function filters the elemenent of the columns str_name with the condition ==
    str_cond
    """

    df = df[df[str_name] == str_cond]
    return df


def df_filter_begin_name(df, str_name, str_filter):
    """
    This function filters the elemenent of the columns str_name which begins with
    str_filter
    """
    return df[df[str_name].str.startswith(str_filter)]


def apply_conditions(row):
    """
    This fucntion creates the elements of a new column where the promos are put
    together in function of some conditions.
    """
    nb_days = "NBJOUR_ODD"
    carticle = "CARTICLE_ODD"
    if row["SG"] == "Semaine généreuse":
        return "Semaine genéreuse"
    elif row[nb_days] == 7:
        return "ODD 7 jours autre que SG"
    elif (row[carticle][:2] == "EV") & ((row[nb_days] == 15) | (row[nb_days] == 14)):
        return "ODD 15 jours EV+"
    elif (row[carticle][:2] == "TC") & ((row[nb_days] == 15) | (row[nb_days] == 14)):
        return "ODD 15 jours TC"
    elif (row[carticle][:2] == "EV") & (row[nb_days] == 21):
        return "ODD 21 jours EV+ "
    elif (row[carticle][:2] == "TC") & (row[nb_days] == 21):
        return "ODD 21 jours TC"
    elif (row[carticle][:2] == "EV") & (row[nb_days] == 30) | (row[nb_days] == 28):
        return "ODD 30 jours EV+"
    elif (row[carticle][:2] == "TC") & (row[nb_days] == 30) | (row[nb_days] == 28):
        return "ODD 30 jours TC"
    else:
        return "Autres"


def create_new_column(df: pd.DataFrame, function):
    """
    This function is used to sendback a new column using the function apply
    and applying function to the elements
    Warning : returns a df of 1 column that is the application of function over each lines.
    """
    return df.apply(function, axis=1)


def keep_used_odd(df_Données_Promos, df_odd, n):
    """
    This function delete all the promos which are not used enough
    """
    df_join_odd = join_dataFrames(df_Données_Promos, df_odd, ["CPROMO"])

    series_count = df_join_odd.groupby(["CPROMO"])[
        "ID_ABONNE"
    ].count()  # count the number of people who used each PROMO
    series_count = series_count.sort_values(
        ascending=False
    )  # sort the number of people
    series_count = series_count[series_count > n]  # Keep the more used
    series_count = series_count.rename(
        "NOMBRE_UTILISATION"
    )  # Rename with a proper name

    df_filtered_groups = series_count.reset_index()  # series to Data fram
    df_new_odd = join_dataFrames(df_filtered_groups, df_odd, ["CPROMO"])

    return df_new_odd


# 3)
# In this part we create some functions to count and give the mean of the datas group by columns and calculating using the condition


def count_abo_conditions(df: pd.DataFrame, columns, cond):
    """
    This function count the number of elements of the condition and group by
    the column
    """
    series = df.groupby(columns)[
        cond
    ].count()  # Group by specified columns and count occurrences of the condition
    df_filtered_groups = series.reset_index(name="NB_" + cond)  # Series into DataFrames

    df_filtered_groups = df_filtered_groups.sort_values(
        by="NB_" + cond, ascending=False
    )  # Sort the DataFrame by the count column in descending order

    return df_filtered_groups


def percent_abo_conditions(df: pd.DataFrame, columns, cond):
    """
    This function count the number of elements of the condition and group by
    the column
    """
    series = df.groupby(columns)[
        cond
    ].count()  # Group by specified columns and count occurrences of the condition
    df_filtered_groups = series.reset_index(
        name="POURCENTAGE_" + cond
    )  # Series into DataFrames

    df_filtered_groups = df_filtered_groups.sort_values(
        by="POURCENTAGE_" + cond, ascending=False
    )  # Sort the DataFrame by the count column in descending order

    n = df_filtered_groups["POURCENTAGE_" + cond].sum()
    df_filtered_groups["POURCENTAGE_" + cond] = (
        df_filtered_groups["POURCENTAGE_" + cond] / n * 100
    )

    df_filtered_groups = df_filtered_groups[
        df_filtered_groups["POURCENTAGE_" + cond] >= 0.01
    ]

    return df_filtered_groups


def percent_abo_conditions_group(df, columns, cond):
    """
    This function count the percentages of elements of the condition and group them by
    the column
    """
    df_grouped = count_abo_conditions(df, columns, cond)
    df_grouped = df_grouped.groupby(columns)["NB_" + cond].sum()
    df_percentage = df_grouped / df_grouped.groupby(columns[0]).transform("sum") * 100

    result_df = df_percentage.reset_index(name="Percentage_" + cond)

    result_df = result_df[result_df["Percentage_" + cond] >= 0.01]

    return result_df


def mean_time_reabo(df, columns, cond):
    """
    This function calculate the mean of the condition and group by
    the column
    """
    series = (
        df.groupby(columns)[cond].mean().round(3)
    )  # Group by specified columns and calculate the mean of the condition
    df_filtered_groups = series.reset_index(name="MEAN_" + cond)

    df_filtered_groups = df_filtered_groups.sort_values(
        by="MEAN_" + cond, ascending=False
    )  # Sort the DataFrame by the mean column in descending order

    return df_filtered_groups


def mean_empty_col(df, column, cond):
    """
    This function calculate the mean of the datas of column col by dividing
    between the datas where the columns cond is empty or not
    """

    series = df.groupby(
        df[column].isna()
    )  # Group by whether the specified column is empty or not, then calculate the mean of the condition
    series = series[cond].mean()
    df_filtered_groups = series.reset_index(name="MEAN_IS_NA" + cond)

    df_filtered_groups = df_filtered_groups.sort_values(
        by="MEAN_IS_NA" + cond, ascending=False
    )  # Sort the DataFrame by the mean column in descending order

    return df_filtered_groups
