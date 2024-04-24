from collections import defaultdict
from itertools import combinations
from kafka import KafkaConsumer

def hash_buckets(itemset, num_buckets):
    return sum(hash(item) for item in itemset) % num_buckets

def generate_candidate_itemsets(transactions, hash_table, k):
    candidates = defaultdict(int)
    if k == 2:
        for transaction in transactions:
            for itemset in combinations(transaction, 2):
                candidates[itemset] += 1
    else:
        for itemset in combinations(hash_table, 2):
            union = set(itemset[0]).union(itemset[1])
            if len(union) == k:
                candidates[tuple(union)] = 0

        for transaction in transactions:
            for candidate in candidates:
                if set(candidate).issubset(set(transaction)):
                    candidates[candidate] += 1

    return candidates

def pcy_consumer(topic, min_support, num_buckets):
    frequent_itemsets = []
    hash_table = []
    k = 1
    consumer = KafkaConsumer(topic, bootstrap_servers=['localhost:9092'])
    
    for message in consumer:
        transaction = json.loads(message.value.decode('utf-8'))
        frequent_itemsets.extend([tuple([item]) for item in transaction])
        while True:
            candidates = generate_candidate_itemsets(frequent_itemsets, hash_table, k + 1)
            frequent_itemsets = [itemset for itemset, count in candidates.items() if count >= min_support]
            if len(frequent_itemsets) == 0:
                break
            print(f"Frequent {k + 1}-itemsets:", frequent_itemsets)
            k += 1

            # Update hash table
            for itemset in frequent_itemsets:
                hash_table[hash_buckets(itemset, num_buckets)] = itemset

if __name__ == "__main__":
    topic = 'preprocessed_data_topic'
    min_support = 2
    num_buckets = 100
    pcy_consumer(topic, min_support, num_buckets)
