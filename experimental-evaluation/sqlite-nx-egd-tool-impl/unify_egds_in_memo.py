#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import csv
import argparse
import sqlite3 as lite
import networkx as nx

# Example: ./unify_egds_in_memo.py /home/vadalog/vadalog-engine-bankitalia/disk/covid/output/22032020/conglomerates_to_unify_22_03_company.csv 
# Transitively unifies the second column of a CSV file.

parser = argparse.ArgumentParser(prog='unify_egds', usage='%(prog)s <csv_file>', description="unifies EGDs on the second column of the CSV file")
parser.add_argument('csv_file', type=str, help='The file containing the two-columns CSV to unify')

def main(): 
    """Takes as input a two-columns CSV, where the first is an ID and the second is a labelled null
    and produces a file where all the labelled nulls are transitively unified in such a way that
    any ID can correspond only to a labelled null. The program corresponds to unfication of EGDs
    in a separable Warded Datalog +/- program.     
    """

    args = parser.parse_args()
    csv_file = args.csv_file
    con = lite.connect('unify_memo.db')

    with con:
        cur = con.cursor()
        cur.execute("DROP TABLE IF EXISTS cong;")
        cur.execute("CREATE TABLE cong(company TEXT, conglomerate TEXT);")
        cur.execute("CREATE INDEX idx1 on cong(conglomerate);")
        
        print("Loading file: " + csv_file)
        with open(csv_file, "r") as f: # CSV file input
            reader = csv.reader(f, delimiter=',') # no header information with delimiter
            for row in reader:
                to_db = [row[0], row[1]] # Appends data from CSV file representing and handling of text
                cur.execute("INSERT INTO cong (company, conglomerate) VALUES(?, ?);", to_db)
            con.commit()

        print("Creating indices")
        cur.execute("CREATE INDEX congl_idx1 on cong(company);")
        cur.execute("CREATE INDEX congl_idx2 on cong(conglomerate);")

        cur.execute("select C1.conglomerate, C2.conglomerate " +
        "from cong C1 join cong C2 on (C1.company = C2.company)" +
        "where C1.conglomerate < C2.conglomerate;")
        to_unify = cur.fetchall()

        print("Building graph")
        G=nx.DiGraph()
        G.add_edges_from(to_unify)

        # here we partition into CC and for each CC
        # we choose a class representative. All the elements in the CC
        # will be replaced by the representative.
        print("Partitioning into CC")
        cc = [list(c) for c in nx.weakly_connected_components(G)]
        update_n = dict()
        for n_list in cc:
            update_n[n_list[0]] = list(n_list[1:])

        print("Unifying")
        for n,n_list in update_n.items():
            n_list_str = str(n_list)
            n_list_str = n_list_str.replace("[","").replace("]","")
            query = "update cong set conglomerate = '" + n + "' where conglomerate in (" + str(n_list_str) + ");"
            #print(query)
            cur.execute(query)

        con.commit()

        print("Saving")
        with open(csv_file + "out", mode='w') as out_file:
            writer = csv.writer(out_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            cur.execute("select distinct company, conglomerate from cong;")
            for company, conglomerate in cur.fetchall():
                    writer.writerow((company, conglomerate))

if __name__== "__main__":
    main()
