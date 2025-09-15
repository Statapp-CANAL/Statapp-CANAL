# Deuxième test clustering

import sys

sys.path.append("data_operations")

from tool_functions.cleaning_data import *  # Import custom data cleaning functions
from sklearn.preprocessing import (
    StandardScaler,
)  # Import StandardScaler for data normalization

from sklearn.cluster import KMeans  # Import KMeans clustering algorithm
from sklearn.metrics import (
    silhouette_samples,
    silhouette_score,
)  # Import silhouette metrics

from viualize_datas import *
from new_data_set_all import *

import matplotlib.cm as cm  # Import colormap for visualizations
import matplotlib.pyplot as plt  # Import plotting library
import numpy as np

from matplotlib.backends.backend_pdf import (
    PdfPages,
)  # Import PdfPages for saving plots to PDF


data_path = ""  # Path to data directory

# Scales the data set


def cluster_data_set(filename, columns, change_inf=np.nan, change_nan=15):
    """
    Load a dataset, perform clustering, and return clustered data.

    Args:
    - filename: Name of the file containing the data.
    - columns: List of column names to include in clustering.
    - change_inf: Value to use for replacing infinity.
    - change_nan: Value to use for replacing NaN.

    Returns:
    - data: DataFrame containing clustered data.
    """
    # Load the dataset from file
    df = file_to_dataframe(filename)

    # Select the specified columns
    data = df[columns]

    # Replace infinite values with specified value
    data.replace([np.inf, -np.inf], change_inf, inplace=True)

    # Normalize the data
    scaler = StandardScaler()
    datas = scaler.fit_transform(data)
    data = pd.DataFrame(datas, columns=data.columns)

    # Select a random 10% sample of the data
    indices = np.random.choice(
        range(len(data)), size=int(len(data) * 0.1), replace=False
    )
    data = data.iloc[indices]

    # Replace NaN values with specified value
    data.replace(np.nan, change_nan, inplace=True)

    return data


# Après avoir utilisé KMeans et avoir obtenu clusters = ...


def data_frame_cluster(data, columns, centers_inv, clusters, data_id_abo):
    """
    Create a DataFrame containing clusters, cluster centers, and subscriber IDs.

    Args:
    - data: DataFrame containing the data.
    - columns: List of column names.
    - centers_inv: Inverse cluster centers (denormalized).
    - clusters: Cluster number assigned to each sample.
    - data_id_abo: DataFrame containing subscriber IDs.

    Returns:
    - df_clusters: DataFrame containing clusters, centers, and percentage IDs.
    """
    # Add cluster labels and subscriber IDs to the data
    data["KMEANS"] = clusters
    data["ID_ABONNE"] = data_id_abo["ID_ABONNE"]

    # Generate DataFrame with cluster information
    df_clusters = percent_abo_conditions(data, "KMEANS", "ID_ABONNE")
    df_clusters = df_clusters.sort_values(by="KMEANS")

    # Round and assign cluster centers to the DataFrame
    centers = np.round(centers_inv, decimals=2)
    for j in range(len(columns)):
        df_clusters[columns[j]] = [centers[i][j] for i in range(len(centers))]

    return df_clusters


def write_df_to_excel(df, file_name, sheet_name="Sheet1"):
    """
    Write a DataFrame to an Excel file.

    Args:
    - df: DataFrame to write to Excel.
    - file_name: Name of the Excel file.
    - sheet_name: Name of the sheet in the Excel file (default is 'Sheet1').
    """
    # Create a writer with the specified file name
    writer = pd.ExcelWriter(file_name, engine="xlsxwriter")

    # Write the DataFrame to the specified sheet
    df.to_excel(writer, sheet_name=sheet_name, index=False)

    # Save and close the Excel file
    writer.save()

    return True


def visualize_silhouette_datas_all(
    filename,
    columns,
    range_n_clusters,
    data_path_results,
    change_inf=np.nan,
    change_nan=15,
):
    """
    Visualize silhouette scores for different numbers of clusters.

    Args:
    - filename: Name of the file containing the data.
    - columns: List of column names to include in clustering.
    - range_n_clusters: List of numbers of clusters to test.
    - data_path_results: Path to save the results.
    - change_inf: Value to use for replacing infinity.
    - change_nan: Value to use for replacing NaN.

    Returns:
    - silhouette_scores: List of silhouette scores.
    """
    # Load the dataset from file
    df = file_to_dataframe(filename)
    data = df[columns]

    # Replace infinite values with specified value
    data.replace([np.inf, -np.inf], change_inf, inplace=True)

    # Normalize the data
    scaler = StandardScaler()
    datas = scaler.fit_transform(data)

    data = pd.DataFrame(datas, columns=data.columns)

    # Select a random 10% sample of the data
    indices = np.random.choice(
        range(len(data)), size=int(len(data) * 0.1), replace=False
    )
    data = data.iloc[indices]
    data_id_abo = df[["ID_ABONNE"]].iloc[indices]

    # Replace NaN values with specified value
    data.replace(np.nan, change_nan, inplace=True)

    silhouette_scores = []

    with PdfPages(data_path_results + "silhouettes_scores.pdf") as pdf:
        for n_clusters in range_n_clusters:

            # Initialize the clusterer with n_clusters value and a random generator
            # seed of 10 for reproducibility.
            clusterer = KMeans(n_clusters, random_state=10)
            clusterer.fit(data)

            centers = clusterer.cluster_centers_
            centers_cluster = np.round(scaler.inverse_transform(centers), decimals=2)

            # Create DataFrame with cluster information
            df_cluster = data_frame_cluster(
                data,
                columns,
                scaler.inverse_transform(centers),
                clusterer.labels_,
                data_id_abo,
            )

            # Write cluster DataFrame to Excel
            write_df_to_excel(
                df_cluster,
                data_path_results + "cluster" + str(n_clusters) + ".xlsx",
                str(n_clusters),
            )

            cluster_labels = clusterer.fit_predict(data)

            silhouette_avg = silhouette_score(data, cluster_labels)

            silhouette_scores.append(silhouette_avg)

            print(
                "For n_clusters =",
                n_clusters,
                "The average silhouette_score is :",
                silhouette_avg,
            )

            fig, ax1 = plt.subplots(1, 1)
            fig.set_size_inches(9, 7)

            ax1.set_xlim([-0.1, 1])
            ax1.set_ylim([0, len(data) + (n_clusters + 1) * 10])

            y_lower = 10
            sample_silhouette_values = silhouette_samples(data, cluster_labels)

            for i in range(n_clusters):

                ith_cluster_silhouette_values = sample_silhouette_values[
                    cluster_labels == i
                ]
                ith_cluster_silhouette_values.sort()
                size_cluster_i = ith_cluster_silhouette_values.shape[0]
                y_upper = y_lower + size_cluster_i
                color = cm.nipy_spectral(float(i) / n_clusters)
                ax1.fill_betweenx(
                    np.arange(y_lower, y_upper),
                    0,
                    ith_cluster_silhouette_values,
                    facecolor=color,
                    edgecolor=color,
                    alpha=0.7,
                )
                ax1.text(-0.8, y_lower + 0.5 * size_cluster_i, str(i))
                y_lower = y_upper + 10

            ax1.set_title(
                "The silhouette plot for the various clusters." + str(n_clusters)
            )
            ax1.set_xlabel("The silhouette coefficient values")
            ax1.set_ylabel("Cluster label")

            ax1.axvline(x=silhouette_avg, color="red", linestyle="--")

            ax1.set_yticks([])
            ax1.set_xticks([0, 0.2, 0.4, 0.6, 0.8, 1])

            pdf.savefig(fig)
            plt.close(fig)

    return silhouette_scores


def trace_silhouette_scores(silhouette_scores, abscisses):

    plt.figure(figsize=(8, 6))
    plt.plot(abscisses, silhouette_scores, marker="o")
    plt.xlabel("Number of clusters")
    plt.ylabel("KMeans Silhouette Score")
    plt.title("Silhouette Score for Different Numbers of Clusters")
    plt.show()

    plt.xticks(abscisses)

    return True


def data_frame_cluster_all(data, columns, centers_inv, clusters, data_id_abo):
    """
    Create a DataFrame containing clusters, cluster centers, and subscriber IDs.

    Args:
    - data: DataFrame containing the data.
    - columns: List of column names.
    - centers_inv: Inverse cluster centers (denormalized).
    - clusters: Cluster number assigned to each sample.
    - data_id_abo: DataFrame containing subscriber IDs.

    Returns:
    - df_clusters: DataFrame containing clusters, centers, and percentage IDs.
    """
    # Add cluster labels and subscriber IDs to the data
    data["KMEANS"] = clusters
    data["ID_ABONNE"] = data_id_abo["ID_ABONNE"]

    # Generate DataFrame with cluster information
    df_clusters = percent_abo_conditions(data, "KMEANS", "ID_ABONNE")
    df_clusters = df_clusters.sort_values(by="KMEANS")

    # Round and assign cluster centers to the DataFrame
    centers = np.round(centers_inv, decimals=2)
    for j in range(len(columns)):
        df_clusters[columns[j]] = [centers[i][j] for i in range(len(centers))]

    return df_clusters


def clustering(filename, columns, data_path_results, change_inf=np.nan, change_nan=5):
    """
    Visualize silhouette scores for different numbers of clusters.

    Args:
    - filename: Name of the file containing the data.
    - columns: List of column names to include in clustering.
    - range_n_clusters: List of numbers of clusters to test.
    - data_path_results: Path to save the results.
    - change_inf: Value to use for replacing infinity.
    - change_nan: Value to use for replacing NaN.

    Returns:
    - silhouette_scores: List of silhouette scores.
    """
    # Load the dataset from file
    df = file_to_dataframe(filename)
    data = df[columns]

    # Replace infinite values with specified value
    data.replace([np.inf, -np.inf], change_inf, inplace=True)

    # Normalize the data
    scaler = StandardScaler()
    datas = scaler.fit_transform(data)

    data = pd.DataFrame(datas, columns=data.columns)
    data_id_abo = df[["ID_ABONNE"]]

    data.replace(np.nan, change_nan, inplace=True)

    clusterer = KMeans(8, random_state=10)
    clusterer.fit(data)

    centers = clusterer.cluster_centers_
    centers_cluster = np.round(scaler.inverse_transform(centers), decimals=2)
    df_cluster = data_frame_cluster(
        data, columns, scaler.inverse_transform(centers), clusterer.labels_, data_id_abo
    )

    save_to_csv_file(df_cluster, data_path_results + "clusterall.csv")

    cluster_labels = clusterer.fit_predict(data)

    df["KMEANS"] = clusterer.labels_
    df = df[["ID_ABONNE", "KMEANS"]]

    save_to_csv_file(df, data_path + "clusters_id_all.csv")

    return df


df = file_to_dataframe(data_path + "fusion_table_final.csv")

clustering(
    data_path + "fusion_table_final.csv",
    [
        "Semaine genéreuse_n_REABOS",
        "ODD 15 jours TC_n_REABOS",
        "SCORE_FIDELITE",
        "ANCIENNETE",
    ],
    data_path,
)
