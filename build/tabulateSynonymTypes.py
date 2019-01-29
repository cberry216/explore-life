readSynonyms = []

with open('synonyms.tsv', 'r') as synonyms, open('tabSynonyms.txt', 'w') as tabSynonyms:
    synonyms.readline()
    for line in synonyms:
        synonym = list(filter(lambda x: x != '|', line.split('\t')))[2]
        if synonym not in readSynonyms:
            readSynonyms.append(synonym)
            tabSynonyms.write(synonym)
            tabSynonyms.write('\n')
