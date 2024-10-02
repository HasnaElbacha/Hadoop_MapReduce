# TP 1: MapReduce - Analyse des Ventes et des Journaux Web

## Introduction

Ce projet utilise Hadoop MapReduce pour traiter et analyser des ensembles de données volumineux. Il comporte deux exercices :
1.1 Analyse des ventes par ville.
1.2 Analyse des ventes par une année donner.
2. Analyse des journaux Web pour extraire des statistiques sur les requêtes par adresse IP.

---
## La configuration de fichier docker-compose.yaml
![Graphique des ventes](https://github.com/user-attachments/assets/54244d27-0fef-4d37-a314-16493de2ea91)

## Lancement de conteneur avec la commande docker-compose up -d
![Graphique des ventes](https://github.com/user-attachments/assets/60f24317-2a8e-4037-a902-1f1d7dd7b8d1)

## Exercice 1.1 : Analyse des Ventes par ville

L'objectif est de développer un job MapReduce pour calculer le total des ventes par ville à partir d'un fichier texte. 

### 1.1.1 création et ajoute le contenu au fichier ventes.txt
![Graphique des ventes](https://github.com/user-attachments/assets/f5ae6f5c-3237-40f9-9319-1a73fcf1cc4c)
### 1.1.2 Affichage de fichier ventes.txt
![Graphique des ventes](https://github.com/user-attachments/assets/b6c62bae-4615-49db-b732-95f2474df4a8)
### 1.1.3 Exécution de fichier jar et le stockage de résultat au dossier output1
![5](https://github.com/user-attachments/assets/7e924098-2d4b-4799-b932-2cb5922957ad)
### 1.1.4 Affichage de résutat
Le résultat stocke dans le fichier part-r-00000
![6](https://github.com/user-attachments/assets/1a6b6e8b-b27d-40e6-aac0-b40a01fe9fa1)

## Exercice 1.2 : Analyse des Ventes par une année donner dans les parametres
### 1.2.1 Exécution de fichier jar avec l'année 2023
![7](https://github.com/user-attachments/assets/6837cce2-9ef4-48be-89fd-e1d45d72b7b1)
### 1.2.2 Affichage de résutat
Le résultat stocke dans le fichier part-r-00000
![8](https://github.com/user-attachments/assets/7f9a61e1-febc-47dd-975a-7e227f9214dc)

## Exercice 2 : Analyse des logs Web

Ce deuxième exercice consiste à analyser un ensemble de fichiers journaux (logs) générés par un serveur web. Chaque ligne contient des informations sur les requêtes HTTP, y compris l'adresse IP du client, la date, le chemin d'accès demandé, et le code de réponse HTTP.

Données d'entrée

```bash
192.168.1.1 - - [12/May/2023:15:30:45 +0000] "GET /page1 HTTP/1.1" 200 1234
192.168.1.2 - - [12/May/2023:15:31:02 +0000] "GET /page2 HTTP/1.1" 404 567
192.168.1.1 - - [12/May/2023:15:32:10 +0000] "GET /page1 HTTP/1.1" 200 789
192.168.1.3 - - [12/May/2023:15:32:35 +0000] "GET /page3 HTTP/1.1" 200 987
```
### 2.1 Création et l'ajoute de contenu au fichier logs.txt
![9 1](https://github.com/user-attachments/assets/93f87ee5-6a72-448f-8ff2-dd98357c9825)
### 2.2 L'exécution de fichier jar 
![9 2](https://github.com/user-attachments/assets/a5373a29-489f-4dc4-8832-15aa314682d4)
### 2.2 Affichage de résultat
![10](https://github.com/user-attachments/assets/d1c645cb-0de6-4077-8df0-b04455e0ad3f)
