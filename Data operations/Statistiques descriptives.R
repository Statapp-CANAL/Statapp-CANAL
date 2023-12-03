library(tidyverse) 
library(lubridate)
library(stringr) 

###

abo2021 <- read.csv("/Users/antoine/Documents/ENSAE 2A/Statapp/ressources/Données_Reabos_2021.csv")
promo2021 <- read.csv("/Users/antoine/Documents/ENSAE 2A/Statapp/ressources/Données_Promos_2021.csv")
liste_promo <- read.csv2("/Users/antoine/Documents/ENSAE 2A/Statapp/ressources/Correspondances_Promos.csv")
new_lp <- read.csv2("/Users/antoine/Documents/ENSAE 2A/Statapp/ressources/Correspondances_Promos_2.csv")

###  Réabonnements

# Nettoyage de la base

abo2021_2 <- abo2021 %>% 
  mutate(DATE_ACTE_REEL = str_sub(DATE_ACTE_REEL,0,10)) %>% 
  mutate(DATE_FIN_REABO = str_sub(DATE_FIN_REABO,0,10)) %>% 
  mutate(FORMULE_PREC = str_sub(FORMULE_PREC,11)) %>% 
  mutate(FORMULE_REABO = str_sub(FORMULE_REABO,11)) %>% 
  select(-REABO_APRES_ECHEANCE) %>% 
  mutate(DATE_ACTE_REEL = ymd(DATE_ACTE_REEL)) %>% 
  mutate(month_acte = month(DATE_ACTE_REEL)) %>% 
  mutate(DATE_PRISE_EFFET = ymd(DATE_PRISE_EFFET)) %>% 
  mutate(month_effet = month(DATE_PRISE_EFFET))

# Nombre de réabonnements dans une année

abo_indiv_2021 <- abo2021_2 %>% group_by(ID_ABONNE) %>% count() 

abo_12_2021 <- abo_indiv_2021 %>% filter(n==12) 
abo_11_2021 <- abo_indiv_2021  %>% filter(n==11)
abo_10_2021 <- abo_indiv_2021  %>% filter(n==10)
abo_9_2021 <- abo_indiv_2021  %>% filter(n==9)
abo_8_2021 <- abo_indiv_2021  %>% filter(n==8)
abo_7_2021 <- abo_indiv_2021  %>% filter(n==7)
abo_6_2021 <- abo_indiv_2021  %>% filter(n==6)
abo_5_2021 <- abo_indiv_2021  %>% filter(n==5)
abo_4_2021 <- abo_indiv_2021  %>% filter(n==4)
abo_3_2021 <- abo_indiv_2021  %>% filter(n==3)
abo_2_2021 <- abo_indiv_2021  %>% filter(n==2)
abo_1_2021 <- abo_indiv_2021  %>% filter(n==1)

# Nombre d'abonnements par mois 

abojanv <- abo2021_2 %>% filter(month_acte == 1) %>% group_by(STATUT_FIN_M_MOINS_1) %>% count()
abofev <- abo2021_2 %>% filter(month_acte == 2) 
abomars <- abo2021_2 %>% filter(month_acte == 3)
aboav <- abo2021_2 %>% filter(month_acte == 4)
abomai <- abo2021_2 %>% filter(month_acte == 5)
abojuin <- abo2021_2 %>% filter(month_acte == 6)
abojuil <- abo2021_2 %>% filter(month_acte == 7)
aboaout <- abo2021_2 %>% filter(month_acte == 8)
abosept <- abo2021_2 %>% filter(month_acte == 9)
abooct <- abo2021_2 %>% filter(month_acte == 10)
abonov <- abo2021_2 %>% filter(month_acte == 11)
abodec <- abo2021_2 %>% filter(month_acte == 12)

## Avant ou après l'échéance

avant_total_2021 <- abo2021_2 %>% filter(REABO_AVANT_ECHEANCE == 1)

apres_total_2021 <- abo2021_2 %>% filter(REABO_AVANT_ECHEANCE == 0)

delai_reabo <- apres_total_2021 %>% filter(DATE_ACTE_REEL != DATE_PRISE_EFFET)

# Détail pour chaque abonné

avant_2021 <- abo2021_2 %>% filter(REABO_AVANT_ECHEANCE == 1) %>% group_by(ID_ABONNE) %>% count() %>% rename(nb_reabo_avant = n) 
apres_2021 <- abo2021_2 %>% filter(REABO_AVANT_ECHEANCE == 0) %>% group_by(ID_ABONNE) %>% count() %>% rename(nb_reabo_apres = n) 

comparaison_av_ap_2021 <- avant_2021 %>% left_join(apres_2021, by = "ID_ABONNE") 

## Statut au mois précedent par abo

statut1_total_2021 <- abo2021_2 %>% filter(STATUT_FIN_M_MOINS_1 == 1)
statut0_total_2021 <- abo2021_2 %>% filter(STATUT_FIN_M_MOINS_1 == 0)

statut_1_2021 <- abo2021_2 %>% filter(STATUT_FIN_M_MOINS_1 == 1) %>% group_by(ID_ABONNE) %>% count() %>% rename(nb_statut_avant = n)
statut_0_2021 <- abo2021_2 %>% filter(STATUT_FIN_M_MOINS_1 == 0) %>% group_by(ID_ABONNE) %>% count() %>% rename(nb_statut_apres = n)

comparaison_statut_2021 <- statut_1_2021 %>% left_join(statut_0_2021, by = "ID_ABONNE") 

# Combinaison
comparaison_total_2021 <- comparaison_statut_2021 %>% left_join(comparaison_av_ap_2021, by ="ID_ABONNE")

## Lieu et secteur 

region <- abo2021_2 %>% group_by(REGION)%>% count() %>% arrange(n)
secteur <- abo2021_2 %>% group_by(SECTEUR) %>% count() %>% arrange(n)

## Distribution

abo2021_distrib <- abo2021_2 %>% group_by(CANAL_DISTRIB)%>% count() %>% arrange(n)

## Enseigne 

autres <- abo2021_2 %>% filter(CANAL_DISTRIB == "Autres") %>% group_by(ENSEIGNE) %>% count() %>% rename(nb_autres = n)
depotlog <- abo2021_2 %>% filter(CANAL_DISTRIB == "Dépôt Logistique") %>% group_by(ENSEIGNE) %>% count() %>% rename(nb_depotlog = n)
FAI <- abo2021_2 %>% filter(CANAL_DISTRIB == "FAI") %>% group_by(ENSEIGNE) %>% count() %>% rename(nb_FAI = n)
partenaires <- abo2021_2 %>% filter(CANAL_DISTRIB == "Réseau Partenaires") %>% group_by(ENSEIGNE) %>% count() %>% rename(nb_particulier = n)
propre <- abo2021_2 %>% filter(CANAL_DISTRIB == "Réseau en propre") %>% group_by(ENSEIGNE) %>% count() %>% rename(nb_propre = n)
nphys <- abo2021_2 %>% filter(CANAL_DISTRIB == "Réseau non physique") %>% group_by(ENSEIGNE) %>% count() %>% rename(nb_nonphysique = n)

## Moyen de payement

pay <- abo2021_2 %>% group_by(MOYEN_PAIEMENT)%>% count() %>% arrange(n) %>% rename(nb_payements = n)

# Partenaire

lieupart <- abo2021_2 %>% group_by(NUMDIST_PARTENAIRE, NOM_PARTENAIRE) %>% count() %>% arrange(n) %>% rename(nb_utilisations = n) 

# Point de vente 

lieuvente <- abo2021_2 %>% group_by(NUMDIST_POINT_DE_VENTE, NOM_POINT_DE_VENTE) %>% count() %>% arrange(n) %>% rename(nb_utilisations = n)

# Promo au sein des réabos 

promoreabo <- abo2021_2 %>% filter(FORMULE_PREC != FORMULE_REABO)
  
### Promos

# Nettoyage 

lp2 <- new_lp %>%  
  mutate(DEBVAL = str_sub(DEBVAL,0,10)) %>% 
  mutate(DEBABOMIN = str_sub(DEBABOMIN,0,10)) %>% 
  mutate(DEBVAL = dmy(DEBVAL)) %>% 
  mutate(DEBABOMIN = dmy(DEBABOMIN)) 

promo2021_2 <- promo2021 %>% 
  mutate(DATE_ACTE_REEL = str_sub(DATE_ACTE_REEL,0,10)) %>% 
  mutate(DATE_DEMARRAGE_PROMO = str_sub(DATE_DEMARRAGE_PROMO,0,10)) 


p2021_join <- promo2021_2 %>% 
  left_join(lp2, by = "CPROMO")  


## Analyses des promos 

reabo_dans_promos <- pro

# Nombre de semaine généreuse 

sg <- p2021_join %>% group_by(SG) %>% count() %>% arrange(n)

# Type de promo

typepromo <- p2021_sg <- p2021_join %>% group_by(TYPE_PROMO) %>% count() %>% arrange(n)

odd <- p2021_join %>% filter(TYPE_PROMO == "ODD")

# Promos les plus achetées 

pr <- p2021_join %>% group_by(CPROMO, LPROMO, AVANTAGE) %>% count() %>% arrange(n)

# Analyse des proportions promos qui ne sont pas des ODD

nodd <- anti_join(p2021_join,odd) 

# Proportion de remise tarifaire vs mois gratuit

a1 <- nodd %>% group_by(AVANTAGE) %>% count() %>% arrange(n)

# Promotions remises tarifaires

a2 <- nodd %>% filter(AVANTAGE == "Remise tarifaire") %>% group_by(CPROMO, LPROMO) %>% count() %>% arrange(n)

# Promos achetées à un jour différent de leur activation 

memejour <- p2021_join %>% filter(DATE_DEMARRAGE_PROMO==DATE_ACTE_REEL)

diff <- anti_join(p2021_join,memejour) 

diff <- diff %>% mutate(DATE_DEMARRAGE_PROMO = as.Date(DATE_DEMARRAGE_PROMO)) %>% 
                 mutate(DATE_ACTE_REEL = as.Date(DATE_ACTE_REEL))

avantpromo <- diff %>% filter(DATE_ACTE_REEL < DATE_DEMARRAGE_PROMO) %>% group_by(CPROMO,LPROMO) %>% count()
aprespromo <- diff %>% filter(DATE_ACTE_REEL > DATE_DEMARRAGE_PROMO) %>% group_by(CPROMO,LPROMO) %>% count()

# Après promo : que des remises tarifaires, sauf une exception !

exception <- aprespromo %>% filter(TYPE_PROMO == "ODD")


indices_doublons <- duplicated(promo2021_2[c("ID_ABONNE", "DATE_ACTE_REEL")]) | duplicated(promo2021_2[c("ID_ABONNE", "DATE_ACTE_REEL")], fromLast = TRUE)
resultat <- promo2021_2[indices_doublons, ] 

aaargh <- abo2021_2 %>% filter(ID_ABONNE == 50402223202) 

