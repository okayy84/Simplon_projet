import pandas as pd
import sqlite3
import requests
import os

# Chemin vers la base dans le volume partagé
db_path = "/db/test.db"

# Connexion à la base
conn = sqlite3.connect(db_path)
curs = conn.cursor()

curs.execute("""
CREATE TABLE IF NOT EXISTS produits (
    id_produit TEXT PRIMARY KEY,
    nom TEXT,
    prix REAL,
    stock INTEGER
);
""")

curs.execute("""
CREATE TABLE IF NOT EXISTS magasins (
    id_magasin INTEGER PRIMARY KEY,
    ville TEXT,
    nb_salaries INTEGER
);
""")

curs.execute("""
CREATE TABLE IF NOT EXISTS ventes (
    id_vente INTEGER PRIMARY KEY AUTOINCREMENT,
    date TEXT,
    id_produit INTEGER,
    quantite INTEGER,
    id_magasin INTEGER,
    FOREIGN KEY (id_produit) REFERENCES produits(id_produit),
    FOREIGN KEY (id_magasin) REFERENCES magasins(id_magasin)
);
""")

# Chargement des données en csv
dico={'ventes' : "https://docs.google.com/spreadsheets/d/e/2PACX-1vSawI56WBC64foMT9pKCiY594fBZk9Lyj8_bxfgmq-8ck_jw1Z49qDeMatCWqBxehEVoM6U1zdYx73V/pub?gid=760830694&single=true&output=csv",
      'produits' : "https://docs.google.com/spreadsheets/d/e/2PACX-1vSawI56WBC64foMT9pKCiY594fBZk9Lyj8_bxfgmq-8ck_jw1Z49qDeMatCWqBxehEVoM6U1zdYx73V/pub?gid=0&single=true&output=csv",
      'magasins' : "https://docs.google.com/spreadsheets/d/e/2PACX-1vSawI56WBC64foMT9pKCiY594fBZk9Lyj8_bxfgmq-8ck_jw1Z49qDeMatCWqBxehEVoM6U1zdYx73V/pub?gid=714623615&single=true&output=csv"}

for nom, url in dico.items():
    reponse=requests.get(url)
    data=reponse.text
    with open(nom+".csv", 'w') as f:
        f.write(data)
        f.close()
    
# renommage des colonnes et correction des types de données
prod = pd.read_csv("produits.csv")
mags = pd.read_csv("magasins.csv")
ventes = pd.read_csv("ventes.csv")

prod.rename(columns={"Nom" : "nom",
             "ID RÃ©fÃ©rence produit" : "id_produit",
             "Prix" : "prix",
             "Stock":"stock"}, inplace=True)

mags.rename(columns={"ID Magasin" : "id_magasin",
             "Ville" : "ville",
             "Nombre de salariÃ©s" : "nb_salaries"}, inplace=True)

ventes.rename(columns={"Date" : "date",
               "ID RÃ©fÃ©rence produit" : "id_produit",
               "QuantitÃ©" : "quantite",
               "ID Magasin" : "id_magasin",
               }, inplace=True)

# Conditions
produit_val_uni = pd.read_sql("SELECT id_produit FROM produits", conn)
magasin_val_uni = pd.read_sql("SELECT id_magasin FROM magasins", conn)
vente_val_uni = pd.read_sql("SELECT id_vente FROM ventes", conn)

ventes['id_vente']=pd.read_sql("SELECT id_vente FROM ventes", conn)['id_vente']

prod_filter = ~prod['id_produit'].isin(produit_val_uni['id_produit'])
mags_filter = ~mags['id_magasin'].isin(magasin_val_uni['id_magasin'])
ventes_filter = ~ventes['id_vente'].isin(vente_val_uni["id_vente"])

# Insertion des valeurs dans les tables à partir des pd.DataFrame 
prod[prod_filter].to_sql("produits", conn, if_exists="append", index=False)
mags[mags_filter].to_sql("magasins", conn, if_exists="append", index=False)
ventes[ventes_filter].to_sql("ventes", conn, if_exists="append", index=False)

conn.commit()
conn.close()