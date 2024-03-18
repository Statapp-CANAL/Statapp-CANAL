import pandas as pd
import math 
import matplotlib.pyplot as plt 
import sys 
sys.path.append("Data_operations")

from Tool_Functions.cleaning_data import file_to_dataframe, save_to_csv_file
from Tool_Functions.join_data import join_dataFrames, join_dataFrames_outer
from Tool_Functions.comportment_reabo import *
from Tool_Functions.visual import *

# 1) 
# In this part we create a new table Correspondance_promos by putting the promo in groups 

def creation_df_odd(data_path, data_path_results):
    """
    This function creates the new df_odd on your Computer where all the ODD are presented and classified with their type
    """

    #data_path = "/Users/maximecoppa/Desktop/Statapp/Datas_clean/" #where to find the datas used
    #data_path_results = "/Users/maximecoppa/Desktop/Statapp/Datas_clean/" #where to create your new file

    df_Correspondances_Promos = file_to_dataframe(data_path + "df_Correspondances_Promos.csv",",") 

    df_odd = df_filter_condition(df_Correspondances_Promos,'TYPE_PROMO','ODD') #we create a DataFrame with only ODD Promotion
    df_odd['TYPE_PROMON'] = create_new_column(df_odd,apply_conditions) #we create new columns on this DataFrame of the ODD type


    for i in [1, 2, 3]:
    #creation df_odd_202i pour chaque année
        df_Données_Promos_202i = file_to_dataframe(data_path + f"df_Données_Promos_202{i}.csv",",")

        n = df_Données_Promos_202i.shape[0] / 10000 #number minimum of used
        df_new_odd = keep_used_odd(df_Données_Promos_202i,df_odd,n) #creation of the new tab by keeping only the used promos
        
        save_to_csv_file(df_new_odd,data_path_results + f"odd_202{i}.csv")
    
    #idem sur les trois années
    df_Données_Promos = file_to_dataframe(data_path + "df_Données_Promos.csv",",")

    n = df_Données_Promos.shape[0] / 10000 #number minimum of used
    df_new_odd = keep_used_odd(df_Données_Promos,df_odd,n) #creation of the new tab by keeping only the used promos
    
    save_to_csv_file(df_new_odd,data_path_results + f"odd.csv")

    return True


#Intermediary step we change the df_Données_Promos by adding a column 'TYPE_PROMON' for being easier to understand instead of 'CPROMO'

def create_df_Données_Promos_odd(data_path, data_path_results):


    for i in [1, 2, 3]:
        #année i
        df_Données_Promos_202i = file_to_dataframe(data_path + f"df_Données_Promos_202{i}.csv",",")
        df_odd = file_to_dataframe(data_path + f"odd_202{i}.csv", ",")
        df_Données_Promos_202i_odd = join_dataFrames(df_Données_Promos_202i,df_odd[['CPROMO','TYPE_PROMON']] ,'CPROMO') #We create a new column 'TYPE_PROMON' on df_Données_Promos_202i
        save_to_csv_file(df_Données_Promos_202i_odd,data_path_results + f"df_Données_Promos_202{i}_odd.csv") #we save it on your Mac

    return True

def create_df_Données_Promos_odd_all(data_path, data_path_results):
    """
    This function create df_Données_Promos_odd for the dataFrame with all years
    """
    
    df_Données_Promos = file_to_dataframe(data_path + "df_Données_Promos.csv",",")
    df_odd = file_to_dataframe(data_path + "odd.csv", ",")
    df_Données_Promos_odd = join_dataFrames(df_Données_Promos,df_odd[['CPROMO','TYPE_PROMON']] ,'CPROMO') #We create a new column 'TYPE_PROMON' on df_Données_Promos_202i
    save_to_csv_file(df_Données_Promos_odd,data_path_results + "df_Données_Promos_odd.csv") #we save it on your Mac
    
    return True

#Intermediary step we create df_Données_Réabos_odd where there are all the Reabos which corresponds to a reabo

def create_df_Données_Reabos_odd_all(data_path, data_path_results):
    """
    This function create df_Données_Reabos_odd with all years of Reabos which corresponds to a use of Promo
    and then we drop some unused column
    """
    df_Données_Promos_odd = file_to_dataframe(data_path +"df_Données_Promos_odd.csv" )
    df_Données_Reabos = file_to_dataframe(data_path + "df_Données_Reabos.csv")
    df_Données_Reabos_odd = join_dataFrames(df_Données_Promos_odd,df_Données_Reabos,['ID_ABONNE','DATE_ACTE_REEL'])

    df_Données_Reabos_odd = df_Données_Reabos_odd.drop(columns = ["REABO_APRES_ECHEANCE","CPROMO","SECTEUR","PAYS","NUMDIST_PARTENAIRE","NOM_PARTENAIRE","NUMDIST_POINT_DE_VENTE","NOM_POINT_DE_VENTE"])

    end_abo = 'DATE_FIN_ABO_PREC'
    date_reabo = 'DATE_ACTE_REEL'

    df_Données_Reabos_odd = time_reabo_columns(df_Données_Reabos_odd,end_abo,date_reabo)
    
    save_to_csv_file(df_Données_Reabos_odd,data_path_results + "df_Données_Reabos_odd.csv")

    return True



#2)
#In this part we create the repartition of the reabos in function of some conditions 
def repartition_reabo_cond(data_path, data_path_results, action = ['write']):
    """
    This function is used to provide some statistics on the reabo habits.
    action is a list of the actions we need to do.
    """

    df_Données_Promos_2021_odd = file_to_dataframe(data_path + "df_Données_Promos_odd.csv") #We open the df_Données_Promos_2021_odd where TYPEPROMO <-> CPROMO
    df_Données_Reabos_2021 = file_to_dataframe(data_path + "df_Données_Reabos.csv")
    df_join = join_dataFrames(df_Données_Promos_2021_odd,df_Données_Reabos_2021,['ID_ABONNE','DATE_ACTE_REEL']) #We join the tables

    df_join = df_mois_annee(df_join,'DATE_ACTE_REEL')

    #We compute some statistcs using count_abo_conditions : this functions count the number of an occurence where the datas are group by conditions 
    df_repartition_promo = count_abo_conditions(df_join,['TYPE_PROMON'],'ID_ABONNE')
    df_type_promo_canaldistrib = count_abo_conditions(df_join,['TYPE_PROMON', 'CANAL_DISTRIB'],'ID_ABONNE')
    df_month_canaldistrib = count_abo_conditions(df_join,['MONTH', 'YEAR', 'CANAL_DISTRIB'],'ID_ABONNE')
    df_repartition_canaldistrib = count_abo_conditions(df_join,['TYPE_PROMON','MONTH', 'YEAR', 'CANAL_DISTRIB'],'ID_ABONNE')
    df_repartition_region = count_abo_conditions(df_join,['TYPE_PROMON','MONTH','YEAR', 'REGION'],'ID_ABONNE')
    df_repartition_secteur = count_abo_conditions(df_join,['TYPE_PROMON','MONTH','YEAR', 'SECTEUR'],'ID_ABONNE')
    df_repartition_enseigne = count_abo_conditions(df_join,['TYPE_PROMON','MONTH','YEAR', 'ENSEIGNE'],'ID_ABONNE')
    df_repartition_moypay = count_abo_conditions(df_join,['TYPE_PROMON','MONTH','YEAR', 'MOYEN_PAIEMENT'],'ID_ABONNE')
    df_repartition_formule = count_abo_conditions(df_join,['TYPE_PROMON','MONTH','YEAR', 'FORMULE_PREC'],'ID_ABONNE')
    
    nouns = ["df_repartition_promo.csv", "type_promo_canaldistrib.csv", "month_canaldistrib.csv", "repartition_canaldistrib.csv", "repartition_region.csv", "repartition_secteur.csv", "repartition_enseigne.csv", "repartition_moypay.csv", "repartition_formule.csv"]
    with open(data_path_results + 'Files_names.txt', "w") as file:
        for element in nouns:
            file.write(f"{element}\n")

    if 'write' in action:
        save_to_csv_file(df_repartition_promo,data_path_results + "df_repartition_promo.csv")
        save_to_csv_file(df_type_promo_canaldistrib,data_path_results + "type_promo_canaldistrib.csv")
        save_to_csv_file(df_month_canaldistrib,data_path_results + "month_canaldistrib.csv")
        save_to_csv_file(df_repartition_canaldistrib,data_path_results + "repartition_canaldistrib.csv")
        save_to_csv_file(df_repartition_region,data_path_results + "repartition_region.csv")
        save_to_csv_file(df_repartition_secteur,data_path_results + "repartition_secteur.csv")
        save_to_csv_file(df_repartition_enseigne,data_path_results + "repartition_enseigne.csv")
        save_to_csv_file(df_repartition_moypay,data_path_results + "repartition_moypay.csv")
        save_to_csv_file(df_repartition_formule,data_path_results + "repartition_formule.csv")

    return True

def stats_percentage_one_cond(data_path,data_path_results):
    
    #open_new df_données_Reabos_odd
    df_join = file_to_dataframe(data_path + "df_Données_Reabos_odd.csv")

    
    #We compute some statistcs using count_abo_conditions : this functions count the number of an occurence where the datas are group by conditions 
    df_repartition_promo = percent_abo_conditions(df_join,['TYPE_PROMON'],'ID_ABONNE')
    df_repartition_canaldistrib = percent_abo_conditions(df_join,['CANAL_DISTRIB'],'ID_ABONNE')
    df_repartition_region = percent_abo_conditions(df_join,['REGION'],'ID_ABONNE')
    df_repartition_enseigne = percent_abo_conditions(df_join,['ENSEIGNE'],'ID_ABONNE')
    df_repartition_moypay = percent_abo_conditions(df_join,['MOYEN_PAIEMENT'],'ID_ABONNE')
    df_repartition_formule = percent_abo_conditions(df_join,['FORMULE_PREC'],'ID_ABONNE')

    save_to_csv_file(df_repartition_promo,data_path_results + "repartition_promo.csv")
    save_to_csv_file(df_repartition_canaldistrib,data_path_results + "repartition_canaldistrib.csv")
    save_to_csv_file(df_repartition_region,data_path_results + "repartition_region.csv")
    save_to_csv_file(df_repartition_enseigne,data_path_results + "repartition_enseigne.csv")
    save_to_csv_file(df_repartition_moypay,data_path_results + "repartition_moypay.csv")
    save_to_csv_file(df_repartition_formule,data_path_results + "repartition_formule_prec.csv")
    
    return True

def stats_percentage_multiple_conds(data_path,data_path_results):
    
    #open_new df_données_Reabos_odd
    df_join = file_to_dataframe(data_path + "df_Données_Reabos_odd.csv")

    
    #We compute some statistcs using count_abo_conditions : this functions count the number of an occurence where the datas are group by conditions 
    df_repartition_canaldistrib = percent_abo_conditions_group(df_join,['TYPE_PROMON','CANAL_DISTRIB'],'ID_ABONNE')
    df_repartition_region = percent_abo_conditions_group(df_join,['TYPE_PROMON','REGION'],'ID_ABONNE')
    df_repartition_enseigne = percent_abo_conditions_group(df_join,['TYPE_PROMON','ENSEIGNE'],'ID_ABONNE')
    df_repartition_moypay = percent_abo_conditions_group(df_join,['TYPE_PROMON','MOYEN_PAIEMENT'],'ID_ABONNE')
    df_repartition_formule = percent_abo_conditions_group(df_join,['TYPE_PROMON','FORMULE_PREC'],'ID_ABONNE')

    save_to_csv_file(df_repartition_canaldistrib,data_path_results + "promo_" + "repartition_canaldistrib.csv")
    save_to_csv_file(df_repartition_region,data_path_results +  "promo_" +"repartition_region.csv")
    save_to_csv_file(df_repartition_enseigne,data_path_results + "promo_" + "repartition_enseigne.csv")
    save_to_csv_file(df_repartition_moypay,data_path_results  + "promo_"+ "repartition_moypay.csv")
    save_to_csv_file(df_repartition_formule,data_path_results + "promo_" + "repartition_formule_prec.csv")
    
    return True

def repartition_time_reabo(data_path,data_path_results):
    """
    Create a data frame where the reabo are classed by time_reabo
    
    """
    df = file_to_dataframe(data_path + "df_Données_Reabos_odd.csv")

    df_repartition_time_reabo = percent_abo_conditions(df,'DELAI_REABO','ID_ABONNE')
    save_to_csv_file(df_repartition_time_reabo,data_path_results + "repartition_time_reabo.csv")

    return True


#3)
#In this part we focus on the time of reabo between two abonnements

def repartition_reabo(data_path, data_path_results):
    """
    This function is used to compute the time of reabo for each type
    """

    df_Données_Promos_2021 = file_to_dataframe(data_path + "df_Données_Promos_2021_odd.csv",",")
    df_Données_Reabos_2021 = file_to_dataframe(data_path + "df_Données_Reabos_2021.csv",",")

    end_abo = 'DATE_FIN_REABO'
    # Ici il y avait "DATE_FIN_ABO_PREC" mais j'ai l'impression que la variable s'appelle plutot "DATE_FIN_REABO"
    date_reabo = 'DATE_ACTE_REEL'

    df_Données_Reabos_2021 = time_reabo_columns(df_Données_Reabos_2021,end_abo,date_reabo) #Creation new colum "TIME_REABO"

    df_Données_Promos_2021 = str_to_date(df_Données_Promos_2021,'DATE_ACTE_REEL') #Preparation join df_Données_Promos_2021 et df_Données_Reabos_2021
    df_join = join_dataFrames(df_Données_Promos_2021,df_Données_Reabos_2021,['ID_ABONNE','DATE_ACTE_REEL']) #Join df_Données_Promos_2021 et df_Données_Reabos_2021

    df_mean_time_reabo_promos = mean_time_reabo(df_join,'TYPE_PROMON','DELAI_REABO')  
    save_to_csv_file(df_mean_time_reabo_promos,data_path_results + "df_mean_time_reabo_promos.csv") # Save on My Mac

    df_join = join_dataFrames_outer(df_Données_Promos_2021,df_Données_Reabos_2021,['ID_ABONNE','DATE_ACTE_REEL']) 
    df_mean_empty = mean_empty_col(df_join,'CPROMO','DELAI_REABO')
    save_to_csv_file(df_mean_empty,data_path_results + "df_mean_empty.csv") # Save on My Mac


    return True



#4)
#In this part we compute the number of time each person  abonned in one year

def taux_consommation(data_path, data_path_results):

    #data_path = "/Users/maximecoppa/Desktop/Statapp/Datas_clean/" #where to find the datas used
    #data_path_results = "/Users/maximecoppa/Desktop/Statapp/Datas_comportement_reabo/" #where to create your new file

    df_Données_Reabos_2021 = file_to_dataframe(data_path + "df_Données_Reabos_2021.csv",",")

    df_nb_abos = count_abo_conditions(df_Données_Reabos_2021,['ID_ABONNE'],'ID_ABONNE')
    df_nb_reabos_2021 = count_abo_conditions(df_nb_abos,['NB_ID_ABONNE'],'NB_ID_ABONNE')
    df_nb_reabos_2021.rename(columns = {"NB_ID_ABONNE": "NB_ABONNEMENTS","NB_NB_ID_ABONNE": "NB_ID_ABONNE"},inplace=True) #Change name for a easier reading


    save_to_csv_file(df_nb_reabos_2021, data_path_results + "df_nb_reabos_2021.csv")

    return True

def create_df_Données_Promos_odd_all_outer(data_path, data_path_results):
    """
    This function create df_Données_Promos_odd for the dataFrame with all years
    """
    
    df_Données_Promos = file_to_dataframe(data_path + "df_Données_Promos.csv",",")
    df_odd = file_to_dataframe(data_path + "odd.csv", ",")
    df_Données_Promos_odd = join_dataFrames_outer(df_Données_Promos,df_odd[['CPROMO','TYPE_PROMON']] ,'CPROMO')
    df_Données_Promos_odd = df_Données_Promos_odd[df_Données_Promos_odd['TYPE_PROMON'].isna()]
    save_to_csv_file(df_Données_Promos_odd,data_path_results + "df_Données_Promos_odd_outer.csv") #we save it on your Mac
    
    return True


def create_df_Données_Reabos_odd_all_outer(data_path, data_path_results):
    """
    This function create df_Données_Reabos_odd with all years of Reabos which corresponds to a use of Promo
    and then we drop some unused column
    """
    df_Données_Promos_odd = file_to_dataframe(data_path +"df_Données_Promos_odd_outer.csv" )
    df_Données_Reabos = file_to_dataframe(data_path + "df_Données_Reabos.csv")
    df_Données_Reabos_odd = join_dataFrames(df_Données_Promos_odd,df_Données_Reabos,['ID_ABONNE','DATE_ACTE_REEL'])

    df_Données_Reabos_odd = df_Données_Reabos_odd.drop(columns = ["REABO_APRES_ECHEANCE","CPROMO","SECTEUR","PAYS","NUMDIST_PARTENAIRE","NOM_PARTENAIRE","NUMDIST_POINT_DE_VENTE","NOM_POINT_DE_VENTE"])

    end_abo = 'DATE_FIN_ABO_PREC'
    date_reabo = 'DATE_ACTE_REEL'

    df_Données_Reabos_odd = time_reabo_columns(df_Données_Reabos_odd,end_abo,date_reabo)
    df_Données_Reabos_odd = df_Données_Reabos_odd[df_Données_Reabos_odd['TYPE_PROMON'].isna()]
    
    save_to_csv_file(df_Données_Reabos_odd,data_path_results + "df_Données_Reabos_odd_outer.csv")

    return True