import pandas as pd
import sys 
sys.path.append("Data_operations")
from Tool_Functions.cleaning_data import *
from Tool_Functions.cleaning_data import *

# Run pour clément
data_path_clement = "/Users/clementgadeau/Statapp/CSV files/"

# Run pour Maxime
data_path_maxime = "/Users/maximecoppa/Desktop/Statapp_Data/Datas_clean/"

# Lancez ça pour télécharger les fichiers csv des df nettoyés, et des df concaténés.
upload_clean(data_path_maxime)
concat_all_years(data_path_maxime)

