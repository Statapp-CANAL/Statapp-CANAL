import sys
sys.path.append("Data_operations")


from Tool_Functions.cleaning_data import * 
from Tool_Functions.comportment_reabo import * 
from Tool_Functions.join_data import * 
from Tool_Functions.test_comportment_reabo import * 


def some_statistics_data_set(data_path):
    """
    Function to perform some statistical analysis on the dataset.

    Parameters:
    - data_path: Path to the directory containing the dataset files.
    """

    # Load dataset files into dataframes
    df_Données_Reabos_odd = file_to_dataframe(data_path + "df_Données_Reabos_odd.csv")
    df_Données_Promos_odd = file_to_dataframe(data_path + "df_Données_Promos_odd.csv")

    # Print statistics based on different conditions about the Reabos and Promos to see the correspondacnces
    print(count_abo_conditions(df_Données_Reabos_odd, ['ID_ABONNE', 'TYPE_PROMON'], 'DATE_ACTE_REEL'))
    print(count_abo_conditions(df_Données_Reabos_odd, ['ID_ABONNE', 'DATE_ACTE_REEL'], 'TYPE_PROMON'))
    print(count_abo_conditions(df_Données_Reabos_odd, ['TYPE_PROMON'], 'ID_ABONNE'))
    print(count_abo_conditions(df_Données_Promos_odd, ['TYPE_PROMON'], 'ID_ABONNE'))

    return True


def visualize_promos_data_set(data_path, df_name, list_promo=["Semaine genéreuse", "ODD 15 jours TC"]):
    """
    Function to visualize promotions data over time.

    Parameters:
    - data_path: Path to the directory containing the dataset files.
    - df_name: Name of the dataframe file.
    - list_promo: List of promotion types to visualize. Default is ["Semaine genéreuse", "ODD 15 jours TC"].

    Returns:
    - None
    """

    # Load dataframe
    df = file_to_dataframe(data_path + df_name + ".csv")

    # Convert 'DATE_ACTE_REEL' to datetime and extract 'mois_annee' column
    df['DATE_ACTE_REEL'] = pd.to_datetime(df['DATE_ACTE_REEL'])
    df['mois_annee'] = df['DATE_ACTE_REEL'].dt.strftime('%Y-%m')

    # Count the number of subscribers for each promotion type and month
    df = count_abo_conditions(df, ["TYPE_PROMON", 'DATE_ACTE_REEL'], 'ID_ABONNE')

    # Plot the evolution of quantity over time
    plt.figure(figsize=(10, 6))
    for el in list_promo:
        sns.lineplot(x='DATE_ACTE_REEL', y='NB_ID_ABONNE', data=df[df["TYPE_PROMON"] == el].sort_values(by='DATE_ACTE_REEL'), label=el)

    plt.title('Evolution of quantity over time')
    plt.xlabel('DATE_ACTE_REEL')
    plt.ylabel('NB_ID_ABONNE')
    plt.legend()
    plt.show()
