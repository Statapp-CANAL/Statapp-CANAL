import sys
sys.path.append("Data_operations")

from Tool_Functions.cleaning_data import * 

from preparation_data_set import * 
from new_data_set import * 
from viualize_datas import * 



data_path_maxime_initiate = "/Users/maximecoppa/Desktop/Statapp_Data/Datas/"
data_path_maxime = "/Users/maximecoppa/Desktop/Statapp_Data/Datas/"


#Initiate
upload_clean(data_path_maxime)
concat_all_years(data_path_maxime)

#Preparation of the data set
creation_df_odd(data_path_maxime,data_path_maxime)
create_df_Données_Promos_odd_all(data_path_maxime,data_path_maxime)
drop_dupplicated_columns_df_Données_Promos(data_path_maxime,data_path_maxime)
create_df_Données_Reabos_odd_all(data_path_maxime,data_path_maxime)

#Creation of the new data set
create_new_data_set(data_path_maxime,data_path_maxime)

#Some Statistics 
some_statistics_data_set(data_path_maxime)
visualize_promos_data_set(data_path_maxime,"df_Données_Reabos_odd")


