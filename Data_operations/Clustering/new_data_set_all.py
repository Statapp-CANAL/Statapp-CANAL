import sys
sys.path.append("Data_operations")

from Tool_Functions.cleaning_data import *
from Tool_Functions.comportment_reabo import * 
from Tool_Functions.join_data import * 

import datetime

"""
data_path = 
data_path_results = 
"""



def create_df_Données_Promos_odd_v2(data_path, data_path_results):

    for i in [1, 2, 3]:
        #année i
        df_Données_Promos_202i = file_to_dataframe(data_path + f"df_Données_Promos_202{i}.csv",",")
        df_odd = file_to_dataframe(data_path + f"odd_202{i}.csv", ",")
        df_Données_Promos_202i_odd = join_dataFrames_outer(df_Données_Promos_202i,df_odd[['CPROMO','TYPE_PROMON']] ,'CPROMO') #We create a new column 'TYPE_PROMON' on df_Données_Promos_202i
        save_to_csv_file(df_Données_Promos_202i_odd,data_path_results + f"df_Données_Promos_202{i}_odd_v2.csv") #we save it on your Mac

    return True

def create_df_Données_Promos_odd_all_v2(data_path, data_path_results):
    """
    This function create df_Données_Promos_odd for the dataFrame with all years
    """
    
    df_Données_Promos = file_to_dataframe(data_path + "df_Données_Promos.csv",",")
    df_odd = file_to_dataframe(data_path + "odd.csv", ",")
    df_Données_Promos_odd = join_dataFrames_outer(df_Données_Promos,df_odd[['CPROMO','TYPE_PROMON']] ,'CPROMO') #We create a new column 'TYPE_PROMON' on df_Données_Promos_202i
    save_to_csv_file(df_Données_Promos_odd,data_path_results + "df_Données_Promos_odd_v2.csv") #we save it on your Mac
    
    return True

def drop_dupplicated_columns_df_Données_Promos_v2(data_path, data_path_results, filename="df_Données_Promos_odd_v2.csv"):
    """
    Drops duplicated columns from 'df_Données_Promos' dataframe.
    """

    # Load 'df' dataframe
    df = file_to_dataframe(data_path + filename) 

    # Define priorities for 'TYPE_PROMON' column for the priorities when someone takes advantage of two promotions the same day
    priorities = {'Semaine genéreuse': 0, 'ODD 7 jours autre que SG': 1, 'Autres': 2, 'ODD 15 jours EV+': 3, 'ODD 21 jours EV+': 4, 'ODD 30 jours EV+': 5, 'ODD 15 jours TC': 6, 'ODD 21 jours TC': 7,'ODD 30 jours TC': 8}

    # Create a new column 'Priorities' based on priorities
    df['Priorities'] = df['TYPE_PROMON'].map(priorities)

    # Sort dataframe by 'Priorities' column in descending order to keep only the best promotion
    df_trie = df.sort_values(by='Priorities', ascending=False)

    # Drop duplicated columns based on specific subset
    df_new = df_trie.drop_duplicates(subset=['ID_ABONNE', 'DATE_ACTE_REEL', 'DATE_DEMARRAGE_PROMO'])
    df_new.drop('Priorities', axis=1, inplace=True)

    # Save 'df_new' dataframe to a CSV file
    save_to_csv_file(df_new, data_path_results + filename)

    return True


def create_df_Données_Reabos_odd_all_v2(data_path, data_path_results):
    """
    This function create df_Données_Reabos_odd with all years of Reabos which corresponds to a use of Promo
    and then we drop some unused column
    """
    df_Données_Promos_odd = file_to_dataframe(data_path +"df_Données_Promos_odd_v2.csv" )
    df_Données_Reabos = file_to_dataframe(data_path + "df_Données_Reabos.csv")
    df_Données_Reabos_odd = join_dataFrames(df_Données_Promos_odd,df_Données_Reabos,['ID_ABONNE','DATE_ACTE_REEL'])

    df_Données_Reabos_odd = df_Données_Reabos_odd.drop(columns = ["REABO_APRES_ECHEANCE","CPROMO","SECTEUR","PAYS","NUMDIST_PARTENAIRE","NOM_PARTENAIRE","NUMDIST_POINT_DE_VENTE","NOM_POINT_DE_VENTE"])

    end_abo = 'DATE_FIN_ABO_PREC'
    date_reabo = 'DATE_ACTE_REEL'

    df_Données_Reabos_odd = time_reabo_columns(df_Données_Reabos_odd,end_abo,date_reabo)
    
    save_to_csv_file(df_Données_Reabos_odd,data_path_results + "df_Données_Reabos_odd_v2.csv")

    return True

def enlever_abos(data_path, data_path_results):
    df = file_to_dataframe(data_path + "df_Données_Reabos_odd_v2.csv")
    df['NB_APPARITIONS'] = df.groupby('ID_ABONNE')['ID_ABONNE'].transform('count')
    df_new = df[(df['NB_APPARITIONS'] < 34) & (df['NB_APPARITIONS'] > 2)]
    save_to_csv_file(df_new, data_path_results + 'df_Données_Reabos_odd_new_v2.csv')
     
    return True

def enlever_nan(data_path, data_path_results):
    df = file_to_dataframe(data_path + "df_Données_Reabos_odd_new_v2.csv")
    df['TYPE_PROMON'].fillna('PAS_ODD', inplace=True)
    save_to_csv_file(df, data_path_results + 'df_Données_Reabos_odd_new_v3.csv')
    return True

def ajout_delai(data_path, data_path_results):
    df = file_to_dataframe(data_path + "df_Données_Reabos_odd_new_v3.csv")
    df['MOY_DELAI'] = df.groupby('ID_ABONNE')['DELAI_REABO'].transform('mean')
    save_to_csv_file(df, data_path_results + 'df_Données_Reabos_odd_new_v3.csv')
    return True


def create_new_data_set_n_reabos_v3(data_path, data_path_results):
    """
    Creates a new dataset 'new_data_set' from 'df_Données_Reabos_odd' dataframe where the columns are the 
    TYPE_PROMON
    """

    # Load 'df_Données_Reabos_odd' dataframe
    df_Données_Reabos_odd = file_to_dataframe(data_path + "df_Données_Reabos_odd_new_v3.csv")

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
    save_to_csv_file(new_data_set, data_path_results + "df_n_reabos_ODD_v3.csv")

    return True


def create_new_data_set_delai_reabo_v3(data_path, data_path_results):
    """
    Creates a new dataset 'new_data_set' from 'df_Données_Reabos_odd' dataframe where the columns are the 
    TYPE_PROMON
    """

    # Load 'df_Données_Reabos_odd' dataframe
    df_Données_Reabos_odd = file_to_dataframe(data_path + "df_Données_Reabos_odd_new_v3.csv")

    # Perform statistical analysis on 'df_Données_Reabos_odd' dataframe
    df = mean_time_reabo(df_Données_Reabos_odd, ['ID_ABONNE','TYPE_PROMON'], 'DELAI_REABO')

    # Create a pivot table from 'df' dataframe we exchange the lines and columns
    new_data_set = df.pivot_table(index='ID_ABONNE', columns='TYPE_PROMON', values='MEAN_DELAI_REABO')

    # Reset index of the pivot table
    new_data_set = new_data_set.reset_index()

    # Replace NaN values with 0
    new_data_set.fillna(math.inf, inplace=True)

    # Convert floating-point values to integers
    new_data_set = new_data_set.astype(float)

    # Save 'new_data_set' dataframe to a CSV file
    save_to_csv_file(new_data_set, data_path_results + "df_n_reabos_mean_time_ODD_v3.csv")

    return True

def create_new_data_set_v3(data_path, data_path_results):
    """
    Creates a new dataset 'new_data_set' from 'df_Données_Reabos_odd' dataframe where the columns are the 
    TYPE_PROMON
    """

    # Load 'df_Données_Reabos_odd' dataframe
    df_Données_Reabos_odd = file_to_dataframe(data_path + "df_Données_Reabos_odd_new_v3.csv")

    # Create df_n_reabos_ODD
    df_n_reabos_ODD = count_abo_conditions(df_Données_Reabos_odd, ['ID_ABONNE', 'TYPE_PROMON'], 'DATE_ACTE_REEL')
    df_n_reabos_ODD = df_n_reabos_ODD.pivot_table(index='ID_ABONNE', columns='TYPE_PROMON', values='NB_DATE_ACTE_REEL')
    df_n_reabos_ODD = df_n_reabos_ODD.reset_index()
    df_n_reabos_ODD.fillna(0, inplace=True)
    df_n_reabos_ODD = df_n_reabos_ODD.astype(int)
    new_column_names = {col: col + '_n_REABOS' if col != 'ID_ABONNE' else col for col in df_n_reabos_ODD.columns}
    df_n_reabos_ODD = df_n_reabos_ODD.rename(columns=new_column_names)


    # Create df_n_reabos_mean_time_ODD
    df_n_reabos_mean_time_ODD = mean_time_reabo(df_Données_Reabos_odd, ['ID_ABONNE','TYPE_PROMON'], 'DELAI_REABO')
    df_n_reabos_mean_time_ODD = df_n_reabos_mean_time_ODD.pivot_table(index='ID_ABONNE', columns='TYPE_PROMON', values='MEAN_DELAI_REABO')
    df_n_reabos_mean_time_ODD = df_n_reabos_mean_time_ODD.reset_index()
    df_n_reabos_mean_time_ODD.fillna(math.inf, inplace=True)
    df_n_reabos_mean_time_ODD = df_n_reabos_mean_time_ODD.astype(float)
    new_column_names = {col: col + '_MEAN_TIME' if col != 'ID_ABONNE' else col for col in df_n_reabos_mean_time_ODD.columns}
    df_n_reabos_mean_time_ODD = df_n_reabos_mean_time_ODD.rename(columns=new_column_names)

    # Join df_n_reabos_mean_time_ODD df_n_reabos_ODD
    df = join_dataFrames(df_n_reabos_ODD,df_n_reabos_mean_time_ODD,"ID_ABONNE")

    # Save 'new_data_set' dataframe to a CSV file
    save_to_csv_file(df, data_path_results + "new_datas_v3.csv")

    return True

def ajouter_differences(df):
    
    promo_types = [col for col in df.columns if 'MEAN_TIME' in col and col != 'MOY_DELAI']
    
    for promo in promo_types:
        new_col_name = promo + '_DIFF'
        df[new_col_name] = df[promo] - df['MOY_DELAI']

    return df

def create_new_data_set_diff_v3(data_path, data_path_results):

    df_Données_Reabos_odd = file_to_dataframe(data_path + "df_Données_Reabos_odd_new_v3.csv")
    df = file_to_dataframe(data_path + "new_datas_v3.csv")
    delai = df_Données_Reabos_odd[['ID_ABONNE', 'MOY_DELAI']]
    delai = delai.drop_duplicates(subset='ID_ABONNE', keep='first')
    df2 = join_dataFrames(df, delai, 'ID_ABONNE')
    df2 = join_dataFrames(df, delai, "ID_ABONNE")
    ajouter_differences(df2)
    
    save_to_csv_file(df2, data_path_results + "new_datas_diff_v3.csv")

    return True

def create_new_data_set_diff_p_v3(data_path, data_path_results):

    df_Données_Reabos_odd = file_to_dataframe(data_path + "df_Données_Reabos_odd_new_v3.csv")
    df = file_to_dataframe(data_path + "new_datas_v3.csv")
    delai = df_Données_Reabos_odd[['ID_ABONNE', 'MOY_DELAI', 'NB_APPARITIONS']]
    delai = delai.drop_duplicates(subset='ID_ABONNE', keep='first')
    
    df2 = join_dataFrames(df, delai, 'ID_ABONNE')

    ajouter_differences(df2)

    df2 = df2.drop(columns= ['Autres_MEAN_TIME', 'ODD 15 jours EV+_MEAN_TIME', 
                             'ODD 15 jours TC_MEAN_TIME',
                             'ODD 21 jours TC_MEAN_TIME', 'ODD 30 jours EV+_MEAN_TIME',
                             'ODD 30 jours TC_MEAN_TIME','ODD 7 jours autre que SG_MEAN_TIME',
                             'Semaine genéreuse_MEAN_TIME'])
    
    col_recensement = ['Autres_n_REABOS', 'ODD 15 jours EV+_n_REABOS',
                        'ODD 15 jours TC_n_REABOS',
                        'ODD 21 jours TC_n_REABOS', 'ODD 30 jours EV+_n_REABOS',
                        'ODD 30 jours TC_n_REABOS','ODD 7 jours autre que SG_n_REABOS',
                          'Semaine genéreuse_n_REABOS']

    for col in col_recensement:
        df2[col + '_POURCENTAGE'] = (df2[col] / df2['NB_APPARITIONS']) * 100

    save_to_csv_file(df2, data_path_results + "new_datas_diff_%_v3.csv")

    return True

def create_new_data_set_n_month_v3(data_path, data_path_results):
    """
    Creates a new dataset 'new_data_set' from 'df_Données_Reabos_odd' dataframe where the columns are the 
    TYPE_PROMON
    """

    # Load 'df_Données_Reabos_odd' dataframe
    df = file_to_dataframe(data_path + "df_Données_Reabos_odd_new_v3.csv")
    df['DATE_ACTE_REEL'] = pd.to_datetime(df['DATE_ACTE_REEL'])
    df['MONTH'] = df['DATE_ACTE_REEL'].dt.month

    # Perform statistical analysis on 'df_Données_Reabos_odd' dataframe
    df2 = count_abo_conditions(df, ['ID_ABONNE', 'MONTH'], 'DATE_ACTE_REEL')

    # Create a pivot table from 'df' dataframe we exchange the lines and columns
    new_data_set = df2.pivot_table(index='ID_ABONNE', columns='MONTH', values='NB_DATE_ACTE_REEL')

    # Reset index of the pivot table
    new_data_set = new_data_set.reset_index()

    # Replace NaN values with 0
    new_data_set.fillna(0, inplace=True)

    # Convert floating-point values to integers
    new_data_set = new_data_set.astype(int)

    # Save 'new_data_set' dataframe to a CSV file
    save_to_csv_file(new_data_set, data_path_results + "df_n_months_ODD_v3.csv")

    return True

def create_new_data_set_n_month_pourc_v3(data_path, data_path_results):
    """
    Creates a new dataset 'new_data_set' from 'df_Données_Reabos_odd' dataframe where the columns are the 
    TYPE_PROMON
    """

    # Load 'df_Données_Reabos_odd' dataframe
    df = file_to_dataframe(data_path + "df_Données_Reabos_odd_new_v3.csv")
    df['DATE_ACTE_REEL'] = pd.to_datetime(df['DATE_ACTE_REEL'])
    df['MONTH'] = df['DATE_ACTE_REEL'].dt.month

    # Perform statistical analysis on 'df_Données_Reabos_odd' dataframe
    df2 = count_abo_conditions(df, ['ID_ABONNE', 'MONTH'], 'DATE_ACTE_REEL')

    # Create a pivot table from 'df' dataframe we exchange the lines and columns
    new_data_set = df2.pivot_table(index='ID_ABONNE', columns='MONTH', values='NB_DATE_ACTE_REEL')

    new_data_set_pourc = new_data_set.apply(lambda x: (x / x.sum()) * 100, axis=1)

    # Reset index of the pivot table
    new_data_set_pourc = new_data_set_pourc.reset_index()

    # Replace NaN values with 0
    new_data_set_pourc.fillna(0, inplace=True)

    # Convert floating-point values to integers
    new_data_set_pourc = new_data_set_pourc.astype(int)

    # Save 'new_data_set' dataframe to a CSV file
    save_to_csv_file(new_data_set_pourc, data_path + "df_n_months_ODD_%_v3.csv")

    return True

def create_new_data_set_n_fidelite_v3(data_path, data_path_results):
    """
    Creates a new dataset 'new_data_set' from 'df_Données_Reabos_odd' dataframe where the columns are the 
    TYPE_PROMON
    """

    # Load 'df_Données_Reabos_odd' dataframe
    df = file_to_dataframe(data_path + "df_Données_Reabos_odd_new_v3.csv")
    df['DATE_ACTE_REEL'] = pd.to_datetime(df['DATE_ACTE_REEL'])
    df['MONTH'] = df['DATE_ACTE_REEL'].dt.month

    # Perform statistical analysis on 'df_Données_Reabos_odd' dataframe
    df2 = count_abo_conditions(df, ['ID_ABONNE', 'STATUT_FIN_M_MOINS_1'], 'DATE_ACTE_REEL')

    # Create a pivot table from 'df' dataframe we exchange the lines and columns
    new_data_set = df2.pivot_table(index='ID_ABONNE', columns='STATUT_FIN_M_MOINS_1', values='NB_DATE_ACTE_REEL')

    # Reset index of the pivot table
    new_data_set = new_data_set.reset_index()

    # Replace NaN values with 0
    new_data_set.fillna(0, inplace=True)

    # Convert floating-point values to integers
    new_data_set = new_data_set.astype(int)

    # Save 'new_data_set' dataframe to a CSV file
    save_to_csv_file(new_data_set, data_path_results + "df_n_fidélité_ODD_v3.csv")

    return True


"""
def correct_non_overlapping_subscriptions(df, window_months=12):
    # Ensure the dataframe is sorted by ID_ABONNE and DATE_ACTE_REEL
    df.sort_values(['ID_ABONNE', 'DATE_ACTE_REEL'], inplace=True)
    df['DATE_ACTE_REEL'] = pd.to_datetime(df['DATE_ACTE_REEL'])

    # Initialize an empty dictionary to store the results
    results = {}

    for id_abonne, group in df.groupby('ID_ABONNE'):
        subscriptions = []
        start_date = None
        count = 0

        for _, row in group.iterrows():
            if start_date is None:
                # Start of a new 12-month period
                start_date = row['DATE_ACTE_REEL']
                count = 1
            elif (row['DATE_ACTE_REEL'] - start_date).days / 30 <= window_months:
                # Still within the 12-month window
                count += 1
            else:
                # Outside the 12-month window, start a new period
                subscriptions.append(count)
                start_date = row['DATE_ACTE_REEL']
                count = 1

        # Append the count for the last window
        subscriptions.append(count)

        # Store the list of counts for this ID_ABONNE
        results[id_abonne] = subscriptions


    return results

""" 

def correct_non_overlapping_subscriptions(data_path, window_months=12):
    df = file_to_dataframe(data_path + "df_Données_Reabos_odd_new_v3.csv")
    # Ensure the dataframe is sorted by ID_ABONNE and DATE_ACTE_REEL
    df.sort_values(['ID_ABONNE', 'DATE_ACTE_REEL'], inplace=True)
    df['DATE_ACTE_REEL'] = pd.to_datetime(df['DATE_ACTE_REEL'])

    # Initialize an empty dictionary to store the results
    results = {}

    for id_abonne, group in df.groupby('ID_ABONNE'):
        subscriptions = []
        start_date = None
        count = 0

        for _, row in group.iterrows():
            if start_date is None:
                # Start of a new 12-month period
                start_date = row['DATE_ACTE_REEL']
                count = 1
            elif (row['DATE_ACTE_REEL'] - start_date).days / 30 <= window_months:
                # Still within the 12-month window
                count += 1
            else:
                # Outside the 12-month window, start a new period
                subscriptions.append(count)
                start_date = row['DATE_ACTE_REEL']
                count = 1

        # Append the count for the last window
        subscriptions.append(count)

        # Store the list of counts for this ID_ABONNE
        results[id_abonne] = subscriptions
        results_df = pd.DataFrame(list(results.items()), columns=['ID_ABONNE', 'NOMBRE_ABONNEMENTS'])
        save_to_csv_file(results_df, data_path + 'liste_fidelite_v4.csv')


    return True


def huge_data_set(data_path, data_path_results):
    df1 = file_to_dataframe(data_path + "new_datas_diff_%_v3.csv")
    df2 = file_to_dataframe(data_path + "df_n_months_ODD_v3.csv")
    df3 = file_to_dataframe(data_path + "liste_fidelite_v3.csv")
    df = join_dataFrames(df1, df2, 'ID_ABONNE')
    dfr = join_dataFrames(df, df3, 'ID_ABONNE')
    save_to_csv_file(dfr, data_path_results + 'fusion_table_v3.csv')
    return True