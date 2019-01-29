readFlags = []

with open('taxonomy.tsv', 'r') as taxonomy, open('tabFlags.txt', 'w') as tabFlags:
    taxonomy.readline()
    for line in taxonomy:
        allFlags = list(filter(lambda x: x != '|', line.split('\t')))[-2]
        flags = allFlags.split(',')
        for flag in flags:
            if flag not in readFlags and flag != '':
                readFlags.append(flag)
                tabFlags.write(flag)
                tabFlags.write('\n')
