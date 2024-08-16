import numpy as np 
from cassandra.cluster import Cluster

class Cassandra:
    def __init__(self, create_keyspace_command):
        self.cluster = Cluster()
        self.session = self.cluster.connect()
        self.session.execute(create_keyspace_command)
        self.session.execute("USE tp")
        self.session.execute("CREATE TABLE IF NOT EXISTS data (id int PRIMARY KEY, value float)")
        self.session.execute("TRUNCATE data")
        self.insert_data()

    def insert_data(self):
        for i in range(10):
            self.session.execute(f"INSERT INTO data (id, value) VALUES ({i}, {np.random.random()})")

    def select_data(self):
        rows = self.session.execute("SELECT * FROM data")
        for row in rows:
            print(row.id, row.value)

    def close(self):
        self.cluster.shutdown()

if __name__ == "__main__":

    # Define the keyspace name, replication strategy, and replication factor
    keyspace_name = 'tp'
    replication_strategy = 'SimpleStrategy'
    replication_factor = 3
    
    create_keyspace_query = f"""
    CREATE KEYSPACE IF NOT EXISTS {keyspace_name}
    WITH replication = {{'class': '{replication_strategy}', 'replication_factor': {replication_factor}}};"""

    cassandra = Cassandra(create_keyspace_query)
    cassandra.select_data()
    cassandra.close()