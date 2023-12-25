import pandas as pd
import matplotlib.pyplot as plt


def generer_graphique(df, categorie_col = None, quantite_col = None, graphique_type='camembert'):
    """
    Génère un graphique à partir d'un DataFrame à deux colonnes.

    Paramètres :
    - df : DataFrame
    - categorie_col : str, nom de la colonne des catégories
    - quantite_col : str, nom de la colonne des quantités
    - graphique_type : str, type de graphique ('camembert' ou 'histogramme')
    """

    # Si le tableau a 2 colonnes, pas besoin de les spécifier.
    if (categorie_col, quantite_col) == (None, None):
        noms_colonnes = df.columns
        categories, categorie_col = df.iloc[:, 0], noms_colonnes[0]
        quantites, quantite_col = df.iloc[:, 1], noms_colonnes[1]

    # Vérifier si les colonnes existent dans le DataFrame
    elif categorie_col not in df.columns or quantite_col not in df.columns:
        raise ValueError("Les colonnes spécifiées ne sont pas présentes dans le DataFrame.")
    
    else :
        categories = df[categorie_col]
        quantites = df[quantite_col]
    
    # Créer le graphique
    if graphique_type == 'camembert':
        plt.pie(quantites, labels=categories, autopct='%1.1f%%', startangle=90)
        plt.title('Répartition de ' + categorie_col)
    elif graphique_type == 'histogramme':
        plt.bar(categories, quantites)
        plt.xlabel(categorie_col)
        plt.ylabel(quantite_col)
        plt.title('Histogramme de' + categorie_col)
    else:
        raise ValueError("Type de graphique non pris en charge. Utilisez 'camembert' ou 'histogramme'.")

    # Afficher le graphique
    plt.show()
    return

