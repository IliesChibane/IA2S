import multiprocessing
from collections import defaultdict
from cassandra.cluster import Cluster
from signal import signal, SIGPIPE, SIG_DFL

# Ignorer le signal SIGPIPE
signal(SIGPIPE, SIG_DFL)

class Cassandra:
    def __init__(self):
        # Initialiser le cluster et la session Cassandra
        self.cluster = Cluster()
        self.session = self.cluster.connect()
        self.session.execute("USE DM")

    def get_text_reviews(self):
        # Récupérer les avis textuels depuis la base de données
        rows = self.session.execute("SELECT text FROM reviews")
        text_reviews = []
        for row in rows:
            text_reviews.append(row.text)

        return text_reviews

    def close(self):
        # Fermer le cluster Cassandra
        self.cluster.shutdown()

def mapper(document):
    """
    Fonction de mapping pour compter les occurrences de blocs de mots dans un document.

    Parameters:
    document (str): Le document à mapper.

    Returns:
    defaultdict: Dictionnaire avec les occurrences des blocs de mots.
    """
    n = 2
    t_mots = document.split()
    m_compteur = defaultdict(int)
    for i in range(len(t_mots) - n + 1):
        blocm = ' '.join(t_mots[i:i+n])
        m_compteur[blocm] += 1
    return m_compteur

def reducer(m_compteur):
    """
    Fonction de réduction pour agréger les résultats du mapping.

    Parameters:
    m_compteur (list): Liste des dictionnaires des occurrences des blocs de mots.

    Returns:
    defaultdict: Dictionnaire agrégé avec les occurrences totales des blocs de mots.
    """
    final_counts = defaultdict(int)
    for counts in m_compteur:
        for blocm, count in counts.items():
            final_counts[blocm] += count
    return final_counts

if __name__ == '__main__':
    # Récupération du dataset via cassandra
    cassandra = Cassandra()
    dataset = cassandra.get_text_reviews()
    cassandra.close()

    # Utiliser un pool de processus pour le mapping
    pool = multiprocessing.Pool()

    # Appliquer la fonction mapper sur le dataset
    mapped_results = pool.map(mapper, dataset)

    # Appliquer la fonction reducer sur les résultats mappés
    reduced_result = reducer(mapped_results)

    # Écrire les résultats dans un fichier texte
    with open("results.txt", "w") as f:
        for blocm, count in reduced_result.items():
            print(f"{blocm}: {count}")
            f.write(f"{blocm}: {count} \n")

    # Fermer le pool de processus
    pool.close()