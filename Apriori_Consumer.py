from collections import defaultdict
from itertools import combinations
from kafka import KafkaConsumer

def generate_candidate_itemsets(transactions, prev_frequent_itemsets, k):
    candidates = defaultdict(int)
    if k == 2:
        for transaction in transactions:
            for itemset in combinations(transaction, 2):
                candidates[itemset] += 1
    else:
        for itemset in combinations(prev_frequent_itemsets, 2):
            union = set(itemset[0]).union(itemset[1])
            if len(union) == k:
                candidates[tuple(union)] = 0

        for transaction in transactions:
            for candidate in candidates:
                if set(candidate).issubset(set(transaction)):
                    candidates[candidate] += 1

    return candidates

def apriori_consumer(topic, min_support):
    frequent_itemsets = []
    k = 1
    consumer = KafkaConsumer(topic, bootstrap_servers=['localhost:9092'])
    
    for message in consumer:
        transaction = json.loads(message.value.decode('utf-8'))
        frequent_itemsets.extend([tuple([item]) for item in transaction])
        while True:
            candidates = generate_candidate_itemsets(frequent_itemsets, frequent_itemsets, k + 1)
            frequent_itemsets = [itemset for itemset, count in candidates.items() if count >= min_support]
            if len(frequent_itemsets) == 0:
                break
            print(f"Frequent {k + 1}-itemsets:", frequent_itemsets)
            k += 1

if __name__ == "__main__":
    topic = 'preprocessed_data_topic'
    min_support = 2
    apriori_consumer(topic, min_support)
