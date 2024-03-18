import sys
sys.path.append("Data_operations")

from Clustering.new_data_set_all import *

data_path = '/Users/clementgadeau/Statapp/CSV files/'
data_path_results = '/Users/clementgadeau/Statapp/CSV files/'

create_df_Données_Promos_odd_v2(data_path, data_path_results)
create_df_Données_Promos_odd_all_v2(data_path, data_path_results)
drop_dupplicated_columns_df_Données_Promos_v2(data_path, data_path_results, filename="df_Données_Promos_odd_v2.csv")
create_df_Données_Reabos_odd_all_v2(data_path, data_path_results)
enlever_abos(data_path, data_path_results)
enlever_nan(data_path, data_path_results)
ajout_delai(data_path, data_path_results)
create_new_data_set_n_reabos_v3(data_path, data_path_results)
create_new_data_set_delai_reabo_v3(data_path, data_path_results)
create_new_data_set_v3(data_path, data_path_results)
ajouter_differences(df)
create_new_data_set_diff_v3(data_path, data_path_results)
create_new_data_set_diff_p_v3(data_path, data_path_results)
create_new_data_set_n_month_v3(data_path, data_path_results)
create_new_data_set_n_month_pourc_v3(data_path, data_path_results)
create_new_data_set_n_fidelite_v3(data_path, data_path_results)
correct_non_overlapping_subscriptions(df, window_months=12)

reabo_v3 = file_to_dataframe(data_path + 'df_Données_Reabos_odd_new_v3.csv')
liste_fidelite = correct_non_overlapping_subscriptions(reabo_v3)
results_df = pd.DataFrame(list(liste_fidelite.items()), columns=['ID_ABONNE', 'NOMBRE_ABONNEMENTS'])
save_to_csv_file(results_df, path_antoine + 'liste_fidelite_v3.csv')

huge_data_set(data_path, data_path_results)