import sys
sys.path.append("Data_operations")

from Tool_Functions.cleaning_data import *
from Tool_Functions.comportment_reabo import * 
from Tool_Functions.join_data import * 



def create_new_data_set(data_path, data_path_results):
    """
    Creates a new dataset 'new_data_set' from 'df_Données_Reabos_odd' dataframe where the columns are the 
    TYPE_PROMON
    """

    # Load 'df_Données_Reabos_odd' dataframe
    df_Données_Reabos_odd = file_to_dataframe(data_path + "df_Données_Reabos_odd.csv")

    # Perform statistical analysis on 'df_Données_Reabos_odd' dataframe
    df = count_abo_conditions(df_Données_Reabos_odd, ['ID_ABONNE', 'TYPE_PROMON'], 'DATE_ACTE_REEL')

    # Create a pivot table from 'df' dataframe we exchange the lines and columns
    new_data_set = df.pivot_table(index='ID_ABONNE', columns='TYPE_PROMON', values='NB_DATE_ACTE_REEL')

    # Reset index of the pivot table
    new_data_set = new_data_set.reset_index()

    # Replace NaN values with 0
    new_data_set.fillna(0, inplace=True)

    # Convert floating-point values to integers
    new_data_set = new_data_set.astype(int)

    # Save 'new_data_set' dataframe to a CSV file
    save_to_csv_file(new_data_set, data_path_results + "new_data_set.csv")

    return True




