import sys
sys.path.append("Data_operations")

from Tool_Functions.cleaning_data import * 

from preparation_data_set import * 
from new_data_set import * 
from viualize_datas import * 
import math 

data_path = "/Users/maximecoppa/Desktop/Statapp_Data/Datas/"


create_new_data_set_n_reabos(data_path,data_path)
create_new_data_set_delai_reabo(data_path,data_path)
create_new_data_set(data_path,data_path)