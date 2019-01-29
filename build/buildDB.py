from os import path, system
import sqlite3 as sql


def formatSQLString(sqlString):
    sqlFormatString = "'"
    for i in range(len(sqlString)):
        if(sqlString[i] == "'"):
            sqlFormatString += "''"
        elif(sqlString[i] != '\n'):
            sqlFormatString += sqlString[i]
    sqlFormatString += "'"
    return sqlFormatString


connection = sql.connect('life.sqlite3')
cursor = connection.cursor()


def dropSynonymTypeTable(connection, cursor):
    cursor.execute('''DROP TABLE IF EXISTS synonym_type;''')
    connection.commit()


def dropSynonymTable(connection, cursor):
    cursor.execute('''DROP TABLE IF EXISTS synonym;''')
    connection.commit()


def dropFlagTypeTable(connection, cursor):
    cursor.execute('''DROP TABLE IF EXISTS flag_type''')
    connection.commit()


def dropFlagTable(connection, cursor):
    cursor.execute('''DROP TABLE IF EXISTS flag;''')
    connection.commit()


def dropRankTable(connection, cursor):
    cursor.execute('''DROP TABLE IF EXISTS rank;''')
    connection.commit()


def dropTaxonTable(connection, cursor):
    cursor.execute('''DROP TABLE IF EXISTS taxon;''')
    connection.commit()


def dropAllTables(connection, cursor):
    dropSynonymTypeTable(connection, cursor)
    dropSynonymTable(connection, cursor)
    dropFlagTypeTable(connection, cursor)
    dropFlagTable(connection, cursor)
    dropRankTable(connection, cursor)
    dropTaxonTable(connection, cursor)
    connection.commit()


def createSynonymTypeTable(connection, cursor):
    # Create 'Synonym Type' table
    cursor.execute('''
    CREATE TABLE synonym_type (
      type_id INTEGER PRIMARY KEY NOT NULL,
      name VARCHAR(256) NOT NULL
    );
    ''')
    connection.commit()


def createSynonymTable(connection, cursor):
    # Create 'Synonym' table
    cursor.execute('''
    CREATE TABLE synonym (
      syn_id INTEGER PRIMARY KEY NOT NULL,
      uid INTEGER NOT NULL,
      name VARCHAR(256) NOT NULL,
      type_id INTEGER NOT NULL,
      FOREIGN KEY (type_id) REFERENCES synonym_type (type_id)
    );
    ''')
    connection.commit()


def createFlagTypeTable(connection, cursor):
    # Create 'Flag Type' table
    cursor.execute('''
    CREATE TABLE flag_type (
      type_id INTEGER PRIMARY KEY NOT NULL,
      name VARCHAR(50) NOT NULL
    )
    ''')


def createFlagTable(connection, cursor):
    # Create 'Flag' table
    cursor.execute('''
    CREATE TABLE flag (
      flag_id INTEGER PRIMARY KEY NOT NULL,
      uid INTEGER NOT NULL,
      type_id INTEGER NOT NULL,
      FOREIGN KEY (type_id) REFERENCES flag_type (type_id)
    );
    ''')
    connection.commit()


def createRankTable(connection, cursor):
    # Create 'Rank' table
    cursor.execute('''
    CREATE TABLE rank (
      rank_id INTEGER PRIMARY KEY NOT NULL,
      name VARCHAR(25) NOT NULL
    );
    ''')
    connection.commit()


def createTaxonTable(connection, cursor):
    # Create 'Taxon' table
    cursor.execute('''
    CREATE TABLE taxon (
      uid INTEGER PRIMARY KEY NOT NULL,
      parent_uid INTEGER,
      name VARCHAR(256) NOT NULL,
      rank_id INTEGER NOT NULL,
      source_info VARCHAR(256),
      FOREIGN KEY (rank_id) REFERENCES rank (rank_id)
    );
    ''')
    connection.commit()


def populateSynonymTypeTable(connection, cursor):
    with open('tabSynonyms.txt', 'r') as synonymTypes:
        for synonymType in synonymTypes:
            cursor.execute("INSERT INTO synonym_type (name) VALUES (" + formatSQLString(synonymType) + ");")
    connection.commit()


def populateSynonymTable(connection, cursor):
    # TODO: Insert synonyms into 'synonym' table
    with open('synonyms.tsv', 'r') as synonyms:
        synonyms.readline()
        currentLine = 0
        for line in synonyms:
            currentLine += 1
            row = list(filter(lambda x: x != '|', line.split('\t')))
            uid = row[1]
            name = row[0]
            typeName = (row[2],)
            cursor.execute('SELECT type_id FROM synonym_type WHERE name=?;', typeName)
            typeID = str(cursor.fetchone()[0])
            attributes = (uid, name, typeID, )
            print('Synonyms:' + str(currentLine) + " / 1846370 : " + str(attributes))
            cursor.execute("INSERT INTO synonym (uid, name, type_id) VALUES (?, ?, ?);", attributes)
    connection.commit()


def populateFlagTypeTable(connection, cursor):
    with open('tabFlags.txt', 'r') as tabFlags:
        for flagType in tabFlags:
            cursor.execute("INSERT INTO flag_type (name) VALUES ('" + flagType.rstrip() + "');")
    connection.commit()


def populateFlagTable(connection, cursor):
    # TODO: Insert flags into 'flag' table
    with open('taxonomy.tsv', 'r') as taxonomy:
        taxonomy.readline()
        currentLine = 0
        for line in taxonomy:
            currentLine += 1
            row = list(filter(lambda x: x != '|', line.split('\t')))
            uid = row[0]
            flags = row[6].split(',')
            if flags[0] != '':
                for flag in flags:
                    cursor.execute("SELECT type_id FROM flag_type WHERE name=?", (flag,))
                    typeID = str(cursor.fetchone()[0])
                    print('   Flags:' + str(currentLine) + " / 3594551 : " + str((uid, flag)))
                    cursor.execute("INSERT INTO flag (uid, type_id) VALUES (?, ?);", (uid, flag,))
    connection.commit()


def populateRankTable(connection, cursor):
    with open('tabRanks.txt', 'r') as tabRanks:
        for rank in tabRanks:
            cursor.execute("INSERT INTO rank (name) VALUES ('" + rank.rstrip() + "')")
    connection.commit()


def populateTaxonTable(connection, cursor):
    # TODO: Insert organisms into 'organism' table
    with open('taxonomy.tsv', 'r') as taxonomy:
        taxonomy.readline()
        currentLine = 0
        for line in taxonomy:
            currentLine += 1
            row = list(filter(lambda x: x != '|', line.split('\t')))

            uid = row[0]
            parentUid = row[1]
            name = row[2]
            rank = row[3]
            sourceInfo = row[4]

            cursor.execute("SELECT rank_id FROM rank WHERE name=?", (rank,))
            rankID = str(cursor.fetchone()[0])

            print('    Taxa:' + str(currentLine) + " / 3594551 : " + str((uid, name)))
            cursor.execute("INSERT INTO taxon (uid, parent_uid, name, rank_id, source_info) VALUES (?,?,?,?,?);",
                           (uid, parentUid, name, rankID, sourceInfo))
    connection.commit()


def buildDatabaseFromScratch(connection, cursor):
    dropAllTables(connection, cursor)

    createSynonymTypeTable(connection, cursor)
    createSynonymTable(connection, cursor)
    createFlagTypeTable(connection, cursor)
    createFlagTable(connection, cursor)
    createRankTable(connection, cursor)
    createTaxonTable(connection, cursor)

    populateSynonymTypeTable(connection, cursor)
    populateSynonymTable(connection, cursor)
    populateFlagTypeTable(connection, cursor)
    populateFlagTable(connection, cursor)
    populateRankTable(connection, cursor)


def runNow(connection, cursor):
    pass


buildDatabaseFromScratch(connection, cursor)

connection.commit()

connection.close()
