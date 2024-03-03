import sys
sys.path.append("Data_operations")



from Tool_Functions.cleaning_data import * 
from Tool_Functions.comportment_reabo import * 
from Tool_Functions.join_data import * 

def creation_df_odd(data_path, data_path_results):
    """
    Creates a new dataframe 'df_odd' containing all the ODD promotions classified by their type.
    """

    # Load 'df_Correspondances_Promos' dataframe
    df_Correspondances_Promos = file_to_dataframe(data_path + "df_Correspondances_Promos.csv", ",") 

    # Filter 'df_Correspondances_Promos' to create 'df_odd' dataframe with only ODD promotions
    df_odd = df_filter_condition(df_Correspondances_Promos, 'TYPE_PROMO', 'ODD')

    # Create a new column 'TYPE_PROMON' based on conditions
    df_odd['TYPE_PROMON'] = create_new_column(df_odd, apply_conditions)

    # Load 'df_Données_Promos' dataframe
    df_Données_Promos = file_to_dataframe(data_path + "df_Données_Promos.csv", ",")

    # Calculate the minimum number of used promotions
    n = df_Données_Promos.shape[0] / 10000

    # Create 'df_new_odd' dataframe by keeping only used promotions
    df_new_odd = keep_used_odd(df_Données_Promos, df_odd, n)
    df_new_odd = df_new_odd.drop_duplicates()

    # Save 'df_new_odd' dataframe to a CSV file
    save_to_csv_file(df_new_odd, data_path_results + "odd.csv")

    return True


def create_df_Données_Promos_odd_all(data_path, data_path_results):
    """
    Creates 'df_Données_Promos_odd' dataframe for all years with promotions data.
    """

    # Load dataframes
    df_Données_Promos = file_to_dataframe(data_path + "df_Données_Promos.csv", ",")
    df_odd = file_to_dataframe(data_path + "odd.csv", ",")

    # Join dataframes to create 'df_Données_Promos_odd' with a new column 'TYPE_PROMON'
    df_Données_Promos_odd = join_dataFrames(df_Données_Promos, df_odd[['CPROMO', 'TYPE_PROMON']], 'CPROMO')
    df_Données_Promos_odd = df_Données_Promos_odd.drop_duplicates()

    # Save 'df_Données_Promos_odd' dataframe to a CSV file
    save_to_csv_file(df_Données_Promos_odd, data_path_results + "df_Données_Promos_odd.csv")

    return True


def drop_dupplicated_columns_df_Données_Promos(data_path, data_path_results, filename="df_Données_Promos_odd.csv"):
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


def create_df_Données_Reabos_odd_all(data_path, data_path_results):
    """
    Creates 'df_Données_Reabos_odd' dataframe with all years of Reabos corresponding to a use of Promo.
    """

    # Load dataframes
    df_Données_Promos_odd = file_to_dataframe(data_path + "df_Données_Promos_odd.csv")
    df_Données_Reabos = file_to_dataframe(data_path + "df_Données_Reabos.csv")

    """
    This part is very controversial and has to be study again
    """
    # Rename columns in 'df_Données_Reabos'
    df_Données_Reabos1 = df_Données_Reabos.rename(columns={'DATE_PRISE_EFFET': 'DATE_DEMARRAGE_PROMO'})

    # Join dataframes to create 'df_Données_Reabos_odd1' with additional columns
    df_Données_Reabos_odd1 = join_dataFrames(df_Données_Promos_odd, df_Données_Reabos1,['ID_ABONNE', 'DATE_ACTE_REEL', 'DATE_DEMARRAGE_PROMO'])
    df_Données_Reabos_odd1['DATE_PRISE_EFFET'] = df_Données_Reabos_odd1['DATE_DEMARRAGE_PROMO']

    # Join dataframes to create 'df_Données_Reabos_odd2' with additional columns
    df_Données_Reabos_odd2 = join_dataFrames(df_Données_Promos_odd, df_Données_Reabos, ['ID_ABONNE', 'DATE_ACTE_REEL'])
    df_Données_Reabos_odd2 = df_Données_Reabos_odd2[df_Données_Reabos_odd2['DATE_PRISE_EFFET'] > df_Données_Reabos_odd2['DATE_DEMARRAGE_PROMO']]

    # Concatenate dataframes to create 'df_Données_Reabos_odd'
    df_Données_Reabos_odd = pd.concat([df_Données_Reabos_odd1, df_Données_Reabos_odd2])

    # Drop unused columns from 'df_Données_Reabos_odd'
    df_Données_Reabos_odd = df_Données_Reabos_odd.drop(columns=["REABO_APRES_ECHEANCE", "CPROMO", "SECTEUR", "PAYS",   "NUMDIST_PARTENAIRE", "NOM_PARTENAIRE",   "NUMDIST_POINT_DE_VENTE", "NOM_POINT_DE_VENTE"])

    # Crate a new colum DELAI_REABO
    end_abo = 'DATE_FIN_ABO_PREC'
    date_reabo = 'DATE_ACTE_REEL'
    df_Données_Reabos_odd = time_reabo_columns(df_Données_Reabos_odd, end_abo, date_reabo)
    df_Données_Reabos_odd = df_Données_Reabos_odd.drop_duplicates()

    # Save 'df_Données_Reabos_odd' dataframe to a CSV file
    save_to_csv_file(df_Données_Reabos_odd, data_path_results + "df_Données_Reabos_odd.csv")

    return True


def enlever_abos(data_path, data_path_results):
    df = file_to_dataframe(data_path + "df_Données_Reabos_odd.csv")
    df['NB_APPARITIONS'] = df.groupby('ID_ABONNE')['ID_ABONNE'].transform('count')
    df_new = df[(df['NB_APPARITIONS'] < 34) & (df['NB_APPARITIONS'] > 2)]
    save_to_csv_file(df_new, data_path_results + 'df_Données_Reabos_odd_new.csv')
     
    return True



def ajouter_differences(df):
    
    promo_types = [col for col in df.columns if 'MEAN_TIME' in col and col != 'MOY_DELAI']
    
    for promo in promo_types:
        new_col_name = promo + '_DIFF'
        df[new_col_name] = df[promo] - df['MOY_DELAI']

    return df