import pandas as pd 
import psycopg2
from sqlalchemy import create_engine
from psycopg2 import OperationalError as e  


def connection(db, user, pwd, host, port):
    try:
        connection = psycopg2.connect(
            database=db,
            user=user,
            password=pwd,
            host=host,
            port=port
            )
        print(f"Connection réussie à {db}")
    except e:
        print(f"Erreur de connection à {db}")
    return connection

conn = connection("transition_eco_dev", "postgres",  "5432")
cursor = conn.cursor()

def create_tables():
    try:
        query = """
        CREATE TABLE IF NOT EXISTS production_electricite_fossile (
            id SERIAL PRIMARY KEY,
            pays_id SMALLINT,
            annee SMALLINT,
            quantite_tep DECIMAL,
            FOREIGN KEY(pays_id) REFERENCES pays(id)
        );

        CREATE TABLE IF NOT EXISTS production_electricite_renouvelable (
            id SERIAL PRIMARY KEY,
            pays_id SMALLINT,
            annee SMALLINT,
            quantite_tep DECIMAL,
            FOREIGN KEY(pays_id) REFERENCES pays(id)
        );

        CREATE TABLE IF NOT EXISTS production_electricite_total (
            id SERIAL PRIMARY KEY,
            pays_id SMALLINT,
            annee SMALLINT,
            quantite_tep DECIMAL,
            FOREIGN KEY(pays_id) REFERENCES pays(id)
        );
        """      
        cursor.execute(query)
        conn.commit()
        print("La création des tables a fonctionné")
    except e:
        print("Erreur dans la création des tables")

def insert_into_production_electricite_fossile_table():
    try:
        production_electricite_fossile = pd.read_csv('data/tables/production_electricite_fossile_table.tsv', sep='\t')
        for row in production_electricite_fossile.itertuples():
            to_db = row[1], row[2], row[3], row[4]
            cursor.execute("""INSERT INTO production_electricite_fossile (id, pays_id, annee, quantite_tep) VALUES (%s, %s, %s, %s);""", to_db)
            conn.commit()
        print("Les données ont bien été ajoutées à la table production_electricite_fossile")
    except e:
        print(f"Les données n'ont pas pu être ajoutées à la table production_electricite_fossile")

def insert_into_production_electricite_renouvelable_table():
    try:
        production_electricite_renouvelable = pd.read_csv('data/tables/production_electricite_renouvelable_table.tsv', sep='\t')
        for row in production_electricite_renouvelable.itertuples():
            to_db = row[1], row[2], row[3], row[4]
            cursor.execute("""INSERT INTO production_electricite_renouvelable (id, pays_id, annee, quantite_tep) VALUES (%s, %s, %s, %s);""", to_db)
            conn.commit()
        print("Les données ont bien été ajoutées à la table production_electricite_renouvelable")
    except e:
        print(f"Les données n'ont pas pu être ajoutées à la table production_electricite_renouvelable")

def insert_into_production_electricite_total_table():
    try:
        production_electricite_total = pd.read_csv('data/tables/production_electricite_total_table.tsv', sep='\t')
        for row in production_electricite_total.itertuples():
            to_db = row[1], row[2], row[3], row[4]
            cursor.execute("""INSERT INTO production_electricite_total (id, pays_id, annee, quantite_tep) VALUES (%s, %s, %s, %s);""", to_db)
            conn.commit()
        print("Les données ont bien été ajoutées à la table production_electricite_total")
    except e:
        print(f"Les données n'ont pas pu être ajoutées à la table production_electricite_total")


if __name__ == "__main__":
    create_tables()
    #insert_into_production_electricite_fossile_table()
    #insert_into_production_electricite_renouvelable_table()
    #insert_into_production_electricite_total_table()
