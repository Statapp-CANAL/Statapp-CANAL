# Canal+ International - Data Science Project
Personalization and Performance of Loyalty Programs in Africa

---

## Project Summary

This project was carried out in collaboration with **Canal+ International** to analyze and improve the personalization and effectiveness of loyalty programs for prepaid subscribers in Africa.

- **Context**: In Senegal, Canal+ operates with prepaid subscriptions where promotions play a crucial role in renewal behavior. The company needed to measure the true effectiveness of loyalty programs and move towards more personalized strategies.  
- **Data**: Over 10 million subscription records (2021-2023) from more than 500,000 subscribers.  
- **Approach**:
  - Built descriptive analyses to understand customer habits and promotion usage.
  - Designed a clustering framework (K-means) to segment subscribers into interpretable groups.
  - Quantified the effect of different promotions on each cluster using multiplicative renewal factors.
  - Validated the results on unseen data from November 2023.  
- **Results**:
  - Identification of distinct customer segments (loyal subscribers, promotion hunters, inactive users, etc.).
  - Clear evidence of which promotions truly drive re-subscriptions in which segments.
  - Business insights enabling Canal+ to reduce unnecessary promotions and target high-value subscribers more effectively.  
- **Impact**:
  - Personalized promotional strategies instead of blanket offers.
  - Better allocation of marketing resources.
  - Improved ROI and long-term customer loyalty.

---

## Code & Implementation

This repository contains the code used to clean and process the data, perform clustering, and validate the results.  
The original datasets and the final insights of the study are **confidential** therefore not included here.

### Repository Structure
```

├── data_operations/
│   ├── clustering/
│   │   └── Scripts for K-means clustering, optimal k selection, validation
│   │
│   ├── tool_function/
│   │   └── Utility functions for data preparation, cleaning, and feature engineering
│   │
│   ├── cluster_prediction_validation.ipynb   
│   └── facteurs_multiplicatifs.ipynb       
│
├── test_generic/                                  # Prototyping and quick experiments
│
└── README.md

```

### Data Preparation and Feature Engineering
- Cleaning and harmonization of multiple raw datasets (subscriptions, promotions, correspondence tables).
- Creation of business-driven features:
  - Renewal delay metrics (how quickly customers renew).
  - Fidelity score (long-term loyalty indicator).
  - Promo responsiveness indicators (frequency and share of use per promotion type).
  - Customer tenure and subscription history.
- Statistical exploration and descriptive insights guided the choice of features.

### Clustering
- **Method**: K-means clustering, selected for efficiency and interpretability.
- **Feature selection**: Fidelity, tenure, promo responsiveness, and renewal behavior.
- **Cluster validation**: Silhouette score, elbow method, bootstrapping for stability.
- **Outcome**: 8 interpretable clusters confirmed with both technical and business validation.

### Results Interpretation
- Clusters were analyzed for promo responsiveness and loyalty behavior.
- Multiplicative factors were computed to measure the relative effectiveness of promotions across clusters and over time.
- Validation on unseen November 2023 data confirmed the robustness of the segmentation.

---

## Authors
- Maxime Coppa, Clément Gadeau, Antoine Gilson  
(ENSAE Paris - StatApp 2023/24, in collaboration with Canal+ International)

