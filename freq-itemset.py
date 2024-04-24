import json
import os
from tqdm import tqdm

def sampleData(inputFile, outputFile, targetSize, filterKey='also_buy'):
    targetSize = targetSize * 1024 * 1024 * 1024
    currentSize = 0

    with open(inputFile, 'r', encoding='utf-8') as input, open(outputFile, 'w', encoding='utf-8') as output:
        for line in tqdm(input):
            data = json.loads(line)
            if data.get(filterKey):
                output.write(json.dumps(data) + '\n')
                currentSize += len(line.encode('utf-8'))

            if currentSize >= targetSize:
                break

sampleData('Sample_Amazon_Meta-small.json', 'Sample_Amazon_Meta33.json', 0.5)




import json
import re

def preprocessing(inputFile, outputFile):
    with open(inputFile, 'r', encoding='utf-8') as f:
        for line in f:
            data = json.loads(line)

            # Preprocess feature field
            feature = data.get('feature', [])
            featureCleaned = ' '.join(feature).lower()
            featureCleaned = re.sub(r'[\d\W_ ]+', '', featureCleaned)

            # Preprocess description field
            description = data.get('description', [])
            descriptionCleaned = ' '.join(description).lower()
            descriptionCleaned = re.sub(r'[\d\W_ ]+', '', descriptionCleaned)

            # Print original and preprocessed data
            print("Original Feature:", feature)
            print("Preprocessed Feature:", featureCleaned)
            print("Original Description:", description)
            print("Preprocessed Description:", descriptionCleaned)

            # Update the data dictionary
            data['feature'] = featureCleaned
            data['description'] = descriptionCleaned

            # Write the preprocessed data to the output file
            with open(outputFile, 'a', encoding='utf-8') as outputFilee:
                outputFilee.write(json.dumps(data) + '\n')

preprocessing('Sample_Amazon_Meta-small.json', 'Preprocessed_Amazon_Meta.json')
