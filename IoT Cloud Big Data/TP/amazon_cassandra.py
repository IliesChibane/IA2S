import pandas as pd
from cassandra.cluster import Cluster

class Cassandra:
    def __init__(self, create_keyspace_command, data):
        # Initialiser le cluster et la session Cassandra
        self.cluster = Cluster()
        self.session = self.cluster.connect()

        # Exécuter la commande de création de l'espace de clés
        self.session.execute(create_keyspace_command)

        # Utiliser l'espace de clés
        self.session.execute("USE DM")

        # Créer la table reviews si elle n'existe pas encore
        self.session.execute("""
            CREATE TABLE IF NOT EXISTS reviews (
                Id int PRIMARY KEY,
                ProductId TEXT,
                UserId TEXT,
                ProfileName TEXT,
                HelpfulnessNumerator int,
                HelpfulnessDenominator int,
                Score int,
                Time int,
                Summary TEXT,
                Text TEXT
            )
        """)
        
        # Tronquer la table reviews
        self.session.execute("TRUNCATE reviews")

        # Insérer les données dans la table reviews
        self.insert_data(data)

    def insert_data(self, data):
        # Lire les données à partir du fichier CSV en utilisant pandas
        df = pd.read_csv(data)

        # Remplir les valeurs manquantes dans les colonnes ProfileName et Summary
        df.ProfileName = df.ProfileName.fillna('Unknown')
        df.Summary = df.ProfileName.fillna('No summary')

        # Itérer à travers les lignes et insérer les données dans Cassandra
        for _, row in df.iterrows():
            id = row["Id"]
            product_id = row["ProductId"]
            user_id = row["UserId"]
            profile_name = row["ProfileName"]
            helpfulness_numerator = row["HelpfulnessNumerator"]
            helpfulness_denominator = row["HelpfulnessDenominator"]
            score = row["Score"]
            time = row["Time"]
            summary = row["Summary"]
            text = row["Text"]

            # Supprimer les guillemets de ProfileName, Summary et Text
            profile_name = profile_name.replace("'", "").replace('"', '')
            summary = summary.replace("'", "").replace('"', '')
            text = text.replace("'", "").replace('"', '')

            # Exécuter la requête INSERT
            self.session.execute(f"""
                INSERT INTO reviews (
                    Id, ProductId, UserId, ProfileName, HelpfulnessNumerator, HelpfulnessDenominator, Score, Time, Summary, Text
                ) VALUES (
                    {id}, '{product_id}', '{user_id}', '{profile_name}', {helpfulness_numerator},
                    {helpfulness_denominator}, {score}, {time}, '{summary}', '{text}'
                )
            """)

    def select_data(self):
        # Sélectionner et afficher toutes les données de la table reviews
        rows = self.session.execute("SELECT * FROM reviews")
        for row in rows:
            print(row)

    def close(self):
        # Fermer le cluster Cassandra
        self.cluster.shutdown()

if __name__ == "__main__":
    # Définir les paramètres de l'espace de clés
    keyspace_name = 'DevoirMaison'
    replication_strategy = 'SimpleStrategy'
    replication_factor = 3
    
    # Créer la requête de création de l'espace de clés
    create_keyspace_query = f"""
        CREATE KEYSPACE IF NOT EXISTS {keyspace_name}
        WITH replication = {{'class': '{replication_strategy}', 'replication_factor': {replication_factor}}};
    """

    # Initialiser l'instance Cassandra, insérer des données, sélectionner des données et fermer la connexion
    cassandra = Cassandra(create_keyspace_query, "Reviews.csv")
    cassandra.select_data()
    cassandra.close()