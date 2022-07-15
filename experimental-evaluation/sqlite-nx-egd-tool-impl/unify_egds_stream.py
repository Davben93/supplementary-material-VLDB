#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import csv
import sys
import argparse
import sqlite3 as lite
import networkx as nx

# Example: ./unify_egds_stream.py /home/vadalog/vadalog-engine-bankitalia/disk/covid/output/22032020/conglomerates_to_unify_22_03_company.csv 
# Transitively unifies the second column of a CSV file.

parser = argparse.ArgumentParser(prog='unify_egds', usage='%(prog)s <csv_file>', description="unifies EGDs on the second column of the CSV file")
parser.add_argument('csv_file', type=str, help='The file containing the two-columns CSV to unify')
COMMIT_RATE=100000 # number of written records before committing

def __splash_screen():
    print("____   ____   ____   ____   ____   ____   ____   ____   ____   ____   ____   ____   ____   ____")   
    print("|____| |____| |____| |____| |____| |____| |____| |____| |____| |____| |____| |____| |____| |____|")  
    print("                                                                                                ")
    print("                                                                                                ")
    print("                                                                                                ")
    print("_______  _______  ______                 __   __  __    _  ___   _______  ___   _______  ______")   
    print("|       ||       ||      |               |  | |  ||  |  | ||   | |       ||   | |       ||    _ |")  
    print("|    ___||    ___||  _    |              |  | |  ||   |_| ||   | |    ___||   | |    ___||   | ||")  
    print("|   |___ |   | __ | | |   |              |  |_|  ||       ||   | |   |___ |   | |   |___ |   |_||_") 
    print("|    ___||   ||  || |_|   |              |       ||  _    ||   | |    ___||   | |    ___||    __  |")
    print("|   |___ |   |_| ||       |              |       || | |   ||   | |   |    |   | |   |___ |   |  | |")
    print("|_______||_______||______|               |_______||_|  |__||___| |___|    |___| |_______||___|  |_|")
    print("                                                                                                ")
    print("                                                                                                ")
    print("____   ____   ____   ____   ____   ____   ____   ____   ____   ____   ____   ____   ____   ____")   
    print("|____| |____| |____| |____| |____| |____| |____| |____| |____| |____| |____| |____| |____| |____|")  
    print("")
    print("")
    sys.stdout.flush()

def __setup_db(con, csv_file, cur):
    """Sets up the working database from the CSV files and
    builds the indices.
    """
    print("Creating DB schema")
    sys.stdout.flush()
    cur.execute("DROP TABLE IF EXISTS cong;")
    cur.execute("CREATE TABLE cong(company TEXT, conglomerate TEXT);")
    cur.execute("CREATE INDEX idx1 on cong(conglomerate);")
    
    print("Loading file: " + csv_file)
    sys.stdout.flush()
    commit = 0
    with open(csv_file, "r") as f: # CSV file input
        reader = csv.reader(f, delimiter=',') # no header information with delimiter
        for row in reader:
            to_db = [row[0], row[1]] # Appends data from CSV file representing and handling of text
            cur.execute("INSERT INTO cong (company, conglomerate) VALUES(?, ?);", to_db)
            commit = commit + 1
            if commit>=COMMIT_RATE:
                con.commit()
                commit=0
        con.commit()

    print("CREATE INDEX congl_idx1 on cong(company);")
    sys.stdout.flush()
    cur.execute("CREATE INDEX congl_idx1 on cong(company);")
    print("CREATE INDEX congl_idx2 on cong(conglomerate);")
    sys.stdout.flush()
    cur.execute("CREATE INDEX congl_idx2 on cong(conglomerate);")
    con.commit()

def __save_db(cur, csv_file):
    """Saves the working database to the output CSV file.
    """
    print("Saving file: " + csv_file + "out")
    sys.stdout.flush()
    with open(csv_file + "out", mode='w') as out_file:
        writer = csv.writer(out_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        cur.execute("select distinct company, conglomerate from cong;")
        for company, conglomerate in cur.fetchall():
                writer.writerow((company, conglomerate))

def __unify(con,cur):
    """Does the real EGD unification. Returns True if at least one unification has
    been applied, False otherwise.
    """

    # the size of dependencies to be updated
    DEP_BATCH_SIZE=10000000

    print("Fetching batch of dependencies")
    cur.execute("select C1.conglomerate, C2.conglomerate " +
    "from cong C1 join cong C2 on (C1.company = C2.company)" +
    "where C1.conglomerate < C2.conglomerate limit " + str(DEP_BATCH_SIZE) + ";")
    to_unify = cur.fetchall()

    print("# dependencies left to unify (in this round): " + str(len(to_unify)))
    sys.stdout.flush()
    # if unification is complete
    if len(to_unify)==0:
        return False

    print("Building the EGD graph")
    sys.stdout.flush()
    G=nx.DiGraph()
    G.add_edges_from(to_unify)

    # here we partition into CC and for each CC
    # we choose a class representative. All the elements in the CC
    # will be replaced by the representative.
    print("Partitioning into CC")
    sys.stdout.flush()
    cc = [list(c) for c in nx.weakly_connected_components(G)]

    print("Unifying EGDs")
    sys.stdout.flush()
    commit = 0
    query = "update cong set conglomerate = ? where conglomerate = ?;"
    data = []

    for n_list in cc:
        for n in n_list[1:]:
                data.append([n_list[0],n])
                commit = commit + 1
                if commit>=COMMIT_RATE:
                    print("Updating DB")
                    sys.stdout.flush()
                    cur.executemany(query,data)
                    con.commit()
                    commit=0
                    data = []

    print("Updating DB")
    sys.stdout.flush()
    cur.executemany(query,data)
    con.commit()
    return True

def main(): 
    """Takes as input a two-column CSV, where the first is an ID and the second is a labelled null
    and produces a file where all the labelled nulls are transitively unified in such a way that
    any ID can correspond only to a labelled null. The program corresponds to unfication of EGDs
    in a separable Warded Datalog +/- program.     
    """

    __splash_screen()

    args = parser.parse_args()
    csv_file = args.csv_file
    con = lite.connect(':memory:')
    #   con = lite.connect('unify_memo.db')
    cur = con.cursor()
    cur.execute('PRAGMA journal_mode = OFF') 


    with con:
        __setup_db(con, csv_file, cur)

        while __unify(con,cur):
            None

        __save_db(cur, csv_file)

if __name__== "__main__":
    main()
