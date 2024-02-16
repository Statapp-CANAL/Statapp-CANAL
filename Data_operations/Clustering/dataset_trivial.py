import pandas as pd
import numpy as np

np.random.seed(0)  # Reproductibilité
nb_lignes = 10000
id_abonne = np.arange(nb_lignes)

# Générer une liste de 10 000 points aléatoires suivant une loi normale centrée en 1
mean = 1
std_dev = 0.5 # Ecart-type

nb_odd_15_tc = np.random.normal(mean, std_dev, nb_lignes)
nb_odd_15_tc = np.clip(nb_odd_15_tc, 0, 2)

nb_odd_15_ev = np.random.normal(mean, std_dev, nb_lignes)
nb_odd_15_ev = np.clip(nb_odd_15_ev, 0, 2)

nb_odd_30_tc = np.random.normal(mean, std_dev, nb_lignes)
nb_odd_30_tc = np.clip(nb_odd_30_tc, 0, 2)


df = pd.DataFrame({
    'ID_ABONNE': id_abonne,
    'NB_ODD_15_TC': nb_odd_15_tc,
    'NB_ODD_15_EV': nb_odd_15_ev,
    'NB_ODD_30_TC': nb_odd_30_tc,
})

print(df.head())
path = '/Users/clementgadeau/Statapp/Clustering/'
df.to_csv(path + 'dataframe_factice.csv')
