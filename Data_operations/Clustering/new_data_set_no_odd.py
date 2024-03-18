import sys
sys.path.append("Data_operations")

from Tool_Functions.cleaning_data import *
from Tool_Functions.comportment_reabo import * 
from Tool_Functions.join_data import * 


def create_new_data_set_delai_reabo_outer(data_path, data_path_results):
    """
    Creates a new dataset 'new_data_set' from 'df_Données_Reabos_odd' dataframe where the columns are the 
    TYPE_PROMON, and includes the 'NB_APPARITIONS' column.
    """

    df_Données_Reabos_odd = file_to_dataframe(data_path + "df_Donnees_Reabos_odd_outer.csv")

    df = mean_time_reabo(df_Données_Reabos_odd, 'ID_ABONNE', 'DELAI_REABO')

    new_data_set = df.pivot_table(index='ID_ABONNE', values='MEAN_DELAI_REABO')

    new_data_set = new_data_set.reset_index()

    nb_apparitions = df_Données_Reabos_odd[['ID_ABONNE', 'NB_APPARITIONS']]

    new_data_set = pd.merge(new_data_set, nb_apparitions, on = 'ID_ABONNE', how='left')

    new_data_set.fillna(math.inf, inplace=True)

    new_data_set = new_data_set.astype(float)

    save_to_csv_file(new_data_set, data_path_results + "df_n_reabos_mean_time_ODD_outer.csv")

    return True
