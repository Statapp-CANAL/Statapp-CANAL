import sys
sys.path.append("Data_operations")

from Clustering.new_data_set_all import *

data_path = "/Users/maximecoppa/Desktop/Statapp_Data/Datas/"
data_path_results = "/Users/maximecoppa/Desktop/Statapp_Data/Datas/"
i = 9
create_df_Données_Promos_odd_v2(data_path, data_path_results)
print(i)
create_df_Données_Promos_odd_all_v2(data_path, data_path_results)
print(i+2)
drop_dupplicated_columns_df_Données_Promos_v2(data_path, data_path_results, filename="df_Données_Promos_odd_v2.csv")
print(i+2)
create_df_Données_Reabos_odd_all_v2(data_path, data_path_results)
print(i+2)
enlever_abos(data_path, data_path_results)
print(i+2)
enlever_nan(data_path, data_path_results)
print(i+2)
ajout_delai(data_path, data_path_results)
print(i+2)
create_new_data_set_n_reabos_v3(data_path, data_path_results)
print(i+2)
create_new_data_set_delai_reabo_v3(data_path, data_path_results)
print(i+2)
create_new_data_set_v3(data_path, data_path_results)
print(i+2)
create_new_data_set_diff_v3(data_path, data_path_results)
print(i+2)
create_new_data_set_diff_p_v3(data_path, data_path_results)
print(i+2)
create_new_data_set_n_month_v3(data_path, data_path_results)
print(i+2)
create_new_data_set_n_month_pourc_v3(data_path, data_path_results)
print(i+2)
create_new_data_set_n_fidelite_v3(data_path, data_path_results)
print(i+2)

print(i+2)
huge_data_set(data_path, data_path_results)
print(i+2)