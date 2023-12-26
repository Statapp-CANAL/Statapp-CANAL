import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
import seaborn as sns


def create_distribution_plot(df, title, label = 0, value = 1, number = True):
    """
    Creates a repartition graph
    Label is the number of the label column, value the number of the value column.
    Number is True if I want to select the columns by their number 
    or False if I want to select them by their label.
    """
    plt.figure()
    if number:
        plt.bar(df.iloc[:, label], df.iloc[:, value])
    else:
        plt.bar(df[label], df[value])
    plt.title(title)
    return


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


def graph_repartition(df, value = 'NB_ID_ABONNE', repartition = 'TYPE_PROMON', through = 'time'):
    """
    Crée un graph à travers through de la quantité value
    et montre sa répartition selon repartion.
    """
    if through == 'time':
        # Convertir MONTH et YEAR en datetime pour faciliter la manipulation du temps
        df['DATE'] = pd.to_datetime(df['MONTH'] + ' ' + df['YEAR'].astype(str))
    # Créer une figure avec deux sous-graphiques
    fig, axes = plt.subplots(2, 1, figsize=(10, 8))

    sns.barplot(x=through, y=value, hue=repartition, data=df, ax=axes[0], palette='muted')
    axes[0].set_title('Répartition de ' + value + ' par' + repartition + 'au fil de ' + through)
    axes[0].legend(title=repartition)

    return


def graph_statdescr(data_path, data_path_results, nouns = 'Files_names.txt'):
    # Lire le fichier et transformer son contenu en liste
    with open(data_path + nouns, "r") as file:
        nouns_list = file.read().splitlines()
        print(nouns_list)

    # Creates a pdf documnent.
    with PdfPages(data_path_results + 'Distribution_report.pdf') as pdf:
        for file in nouns_list:
            df = pd.read_csv(data_path + file,delimiter=',')
            len = df.shape[1]
            
            if len == 2:
                # Add graphs to pdf.
                create_distribution_plot(df, f'{df.columns[1]} par {df.columns[0]}')
                pdf.savefig()
                plt.close()
            elif len == 3:
                labels = df.columns.tolist()
                list = [i for i in labels if i not in ['NB_ID_ABONNE']]
                print(list)
                graph_repartition(df, value = 'NB_ID_ABONNE', repartition = list[0], through = list[1])
                graph_repartition(df, value = 'NB_ID_ABONNE', repartition = list[1], through = list[0])

            elif len >= 4:
                labels = df.columns.tolist()
                list = [i for i in labels if i not in ['NB_ID_ABONNE', 'MONTH', 'YEAR']]
                for i in list:
                    for j in list:
                        if i != j:
                            graph_repartition(df, value = 'NB_ID_ABONNE', repartition = i, through = 'time')

            # Ajuster la disposition
        plt.tight_layout()
        pdf.savefig()
        plt.close()
    return 