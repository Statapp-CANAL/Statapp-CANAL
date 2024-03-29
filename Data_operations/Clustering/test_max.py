import sys
sys.path.append("Data_operations")

from Tool_Functions.cleaning_data import * 

from preparation_data_set import * 
from new_data_set import * 
from viualize_datas import * 
from new_data_set_all import *

import math 

data_path = "/Users/maximecoppa/Desktop/Statapp_Data/Datas/"

"""
df = file_to_dataframe(data_path + 'df_Données_Reabos_odd_new_v3.csv')
print(1)
liste = correct_non_overlapping_subscriptions(df)
print(2)
results_df = pd.DataFrame(list(liste.items()), columns=['ID_ABONNE', 'NOMBRE_ABONNEMENTS'])
print(3)
save_to_csv_file(results_df, data_path + 'liste_fidelite_v3.csv')

"""

huge_data_set(data_path, data_path)