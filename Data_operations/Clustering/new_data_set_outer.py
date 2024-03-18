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

    df_Données_Reabos_odd = file_to_dataframe(data_path + "df_Données_Reabos_odd_outer.csv")

    df_Données_Reabos_odd['NB_APPARITIONS_noODD'] = df_Données_Reabos_odd.groupby('ID_ABONNE')['ID_ABONNE'].transform('count')

    df = mean_time_reabo(df_Données_Reabos_odd, 'ID_ABONNE', 'DELAI_REABO')

    new_data_set = df.pivot_table(index='ID_ABONNE', values='MEAN_DELAI_REABO')

    new_data_set = new_data_set.reset_index()

    nb_apparitions = df_Données_Reabos_odd[['ID_ABONNE', 'NB_APPARITIONS_noODD']]

    new_data_set = pd.merge(new_data_set, nb_apparitions, on = 'ID_ABONNE', how='left')

    new_data_set.fillna(math.inf, inplace=True)

    new_data_set = new_data_set.astype(float)

    save_to_csv_file(new_data_set, data_path_results + "df_n_reabos_mean_time_ODD_outer.csv")

    return True

def new_datas_join(data_path, data_path_results):

    df = file_to_dataframe(data_path + 'df_n_reabos_mean_time_ODD_outer.csv')
    df.rename(columns={'NB_APPARITIONS_noODD': 'noODD_n_REABOS'}, inplace=True)
    df.rename(columns={'MEAN_DELAI_REABO': 'noODD_MEAN_TIME'}, inplace=True)
    df2 = file_to_dataframe(data_path + 'new_datas_join.csv')
    df3 = join_dataFrames(df2, df, 'ID_ABONNE')
    save_to_csv_file(df3, data_path_results + 'new_datas_join_outer.csv')
    return True

def ajouter_differences(df):
    
    promo_types = [col for col in df.columns if 'MEAN_TIME' in col and col != 'MOY_DELAI']
    
    for promo in promo_types:
        new_col_name = promo + '_DIFF'
        df[new_col_name] = df[promo] - df['MOY_DELAI']

    return df

def new_datas_diff_outer(data_path, data_path_results):

   df = file_to_dataframe(data_path + 'new_datas_join_outer.csv')

   columns_delai = [col for col in df.columns if col.endswith('TIME')]


   df[columns_delai] = df[columns_delai].replace([np.inf, -np.inf], np.nan)


   df['MOY_DELAI'] = df[columns_delai].mean(axis=1, skipna=True)

   ajouter_differences(df)

   df = df.drop(columns= ['Autres_MEAN_TIME', 'ODD 15 jours EV+_MEAN_TIME', 
                             'ODD 15 jours TC_MEAN_TIME',
                             'ODD 21 jours TC_MEAN_TIME', 'ODD 30 jours EV+_MEAN_TIME',
                             'ODD 30 jours TC_MEAN_TIME','ODD 7 jours autre que SG_MEAN_TIME',
                             'Semaine genéreuse_MEAN_TIME',
                             'noODD_MEAN_TIME'])

   df = df.drop(columns = ['Autres_n_REABOS', 'ODD 15 jours EV+_n_REABOS',
                        'ODD 15 jours TC_n_REABOS',
                        'ODD 21 jours TC_n_REABOS', 'ODD 30 jours EV+_n_REABOS',
                        'ODD 30 jours TC_n_REABOS','ODD 7 jours autre que SG_n_REABOS',
                          'Semaine genéreuse_n_REABOS', 'noODD_n_REABOS'])

   save_to_csv_file(df, data_path_results + 'new_datas_diff_outer')

   return True