readRanks = []

with open('taxonomy.tsv', 'r') as taxonomy, open('tabRanks.txt', 'w') as tabRanks:
    taxonomy.readline()
    for line in taxonomy:
        rank = list(filter(lambda x: x != '|', line.split('\t')))[3]
        if rank not in readRanks:
            readRanks.append(rank)
            tabRanks.write(rank)
            tabRanks.write('\n')
