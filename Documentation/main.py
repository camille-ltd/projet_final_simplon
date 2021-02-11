import pandas as pd 
import psycopg2
from sqlalchemy import create_engine
from psycopg2 import OperationalError as e  

def connection():
    try:
        db = create_engine('postgres+psycopg2://camille:wn3n87tx@94.23.7.141:5432/transition_energetique')
        engine = create_engine(db)
        print('Connection à la db réussie')
    except e:
            print('Erreur lors de la connection à la db')
    return engine

# def create_tables():
#     try:
#         engine = create_engine('postgres+psycopg2://camille:wn3n87tx@94.23.7.141:5432/transition_energetique')
#         engine.execute("""
#         CREATE TABLE IF NOT EXISTS pays (
#             id SERIAL PRIMARY KEY,
#             code VARCHAR(10),
#             libelle TEXT
#         );

#         CREATE TABLE IF NOT EXISTS population (
#             id SERIAL PRIMARY KEY,
#             pays_id SMALLINT,
#             annee SMALLINT,
#             population INTEGER,
#             FOREIGN KEY(pays_id) REFERENCES pays(id)
#         );

#         CREATE TABLE IF NOT EXISTS code_bilan (
#             code TEXT,
#             libelle TEXT
#         );

#         CREATE TABLE IF NOT EXISTS code_siec (
#             code VARCHAR(10),
#             libelle TEXT
#         );

#         CREATE TABLE IF NOT EXISTS bilan_energetique (
#             id SERIAL PRIMARY KEY,
#             pays_id SMALLINT,
#             annee SMALLINT,
#             siec VARCHAR(25),
#             nrg_bal TEXT,
#             quantite_tep DECIMAL,
#             FOREIGN KEY(pays_id) REFERENCES pays(id),
#             FOREIGN KEY(siec) REFERENCES code_siec(code),
#             FOREIGN KEY(nrg_bal) REFERENCES code_bilan(code)
#         );

#         CREATE TABLE IF NOT EXISTS production_electricite_fossile (
#             id SERIAL PRIMARY KEY,
#             pays_id SMALLINT,
#             annee SMALLINT,
#             quantite_tep DECIMAL,
#             FOREIGN KEY(pays_id) REFERENCES pays(id)
#         );

#         CREATE TABLE IF NOT EXISTS production_electricite_renouvelable (
#             id SERIAL PRIMARY KEY,
#             pays_id SMALLINT,
#             annee SMALLINT,
#             quantite_tep DECIMAL,
#             FOREIGN KEY(pays_id) REFERENCES pays(id)
#         );

#         CREATE TABLE IF NOT EXISTS production_electricite_total (
#             id SERIAL PRIMARY KEY,
#             pays_id SMALLINT,
#             annee SMALLINT,
#             quantite_tep DECIMAL,
#             FOREIGN KEY(pays_id) REFERENCES pays(id)
#         );
#         """)
#     except e:
#         print("Erreur dans la création des tables")

def insert_into_pays_table():
    try:
        engine = create_engine('postgres+psycopg2://camille:wn3n87tx@94.23.7.141:5432/transition_energetique')
        pays = pd.read_csv('data/tables/pays_table.tsv', sep='\t')
        pays.to_sql('pays', engine, if_exists='append', index=False)
        print(f"{pays} l'insertion a fonctionné")
    except e:
            print(f"{pays}, Erreur dans l'insertion du fichier")
    
if __name__ == "__main__":
    #connection()
    #create_tables()
    insert_into_pays_table()