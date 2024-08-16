import multiprocessing
from datasets import load_dataset

# Mapper function
def mapper(document):
    words = document.split()
    word_count = {}
    for word in words:
        word = word.lower()
        if word.isalpha():  # Check if the word contains only alphabets
            if word in word_count:
                word_count[word] += 1
            else:
                word_count[word] = 1
    return word_count

# Reducer function
def reducer(word_counts):
    final_word_count = {}
    for word_count in word_counts:
        for word, count in word_count.items():
            if word in final_word_count:
                final_word_count[word] += count
            else:
                final_word_count[word] = count
    return final_word_count

if __name__ == '__main__':
    # Sample documents
    documents = load_dataset("imdb")

    documents = documents["train"]["text"]

    print(len(documents))
    
    # Create a multiprocessing pool
    pool = multiprocessing.Pool()

    # Map step: Apply mapper function to each document
    mapped_results = pool.map(mapper, documents)

    # Reduce step: Aggregate word counts
    reduced_result = reducer(mapped_results)

    # Print final word count
    for word, count in reduced_result.items():
        print(f"{word}: {count}")