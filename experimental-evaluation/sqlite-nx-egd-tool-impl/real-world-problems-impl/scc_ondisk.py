#!/usr/bin/python

import sys
import egd_common as ec
import sqlite_common as sc
import csv
import time

# SQL scripts

create_edges_table_sql = "create table if not exists edges(nodeFrom text, nodeTo text);"
create_nodes_table_sql = "create table if not exists nodes(node text);"
create_scc_table_sql = "create table if not exists scc(node text, sccLabel text);"

nodes_view_sql = "SELECT * FROM nodes;"
edges_view_sql = "SELECT * FROM edges;"
scc_view_sql = "SELECT * FROM scc;"
delta_path_sql = "SELECT * FROM deltaPath;"
scc_view_drop_sql = "DELETE FROM scc;"

create_path_table_sql = "create table if not exists path(nodeSource text, nodeTarget text);"
create_delta_path_view_sql = "create table deltaPath(nodeSource text, nodeTarget text);"
create_delta_path_view_2_sql = "create table deltaPathApp(nodeSource text, nodeTarget text);"

# edge(X,Y) -> SCC(X,Z),SCC(Y,W). OR
# node(X) -> SCC(X,Z).
tgd_1_sql = "INSERT INTO scc SELECT n.node, \'zXXX\' || n.node FROM nodes as n;"
# edge(X,Y) -> path(X,Y).
tgd_2_sql = "INSERT INTO path SELECT e.nodeFrom, e.nodeTo FROM edges as e;"

# path(X,Y),edge(Y,Z) -> path(X,Z).
tgd_3_1_sql = "INSERT INTO deltaPath SELECT p.nodeSource, p.nodeTarget FROM path AS p;"

# ITERATION
tgd_3_2_sql = "INSERT INTO deltaPathApp \
               SELECT DISTINCT dp.nodeSource, e.nodeTo \
               FROM deltaPath AS dp, edges AS e \
               WHERE dp.nodeTarget=e.nodeFrom; "

tgd_3_3_sql = "DELETE FROM deltaPath;"

tgd_3_4_sql = "INSERT INTO deltaPath \
               SELECT * \
               FROM deltaPathApp \
               EXCEPT \
               SELECT * \
               FROM path; "

tgd_3_5_sql = "DELETE FROM deltaPathApp;"

tgd_3_6_sql = "INSERT INTO path \
               SELECT * \
               FROM deltaPath; "

# SCC(X,Z1),path(X,Y),path(Y,X),SCC(Y,Z2) -> Z1=Z2
egd_sql = "SELECT s1.sccLabel, s2.sccLabel \
           FROM path AS p1, path AS p2, scc AS s1, scc AS s2 \
           WHERE p1.nodeSource=p2.nodeTarget AND p2.nodeSource=p1.nodeTarget \
           AND p1.nodeSource=s1.node AND p1.nodeTarget=s2.node; "

bcq_is_scc = "SELECT s1.node, s2.node \
              FROM scc AS s1, scc AS s2 \
              WHERE s1.sccLabel <> s2.sccLabel;"

cq_same_scc = "SELECT s1.node, s2.node \
               FROM scc AS s1, scc AS s2 \
               WHERE s1.sccLabel = s2.sccLabel;"


def unify_labels(scc, cc, connection):
    for part, l in cc.items():
        for n, label in scc:
            if label in l or label == part:
                cur = connection.cursor()
                cur.execute("insert into " + "scc" + " values (?,?);", (n, part))
    connection.commit()


def do_chase_and_run_queries_on_disk(input_filename: str, output_filename):
    # create a connection to an in memory database
    connection = sc.create_connection_persistent("app/scc.db")

    # create the schemas for the predicates
    sc.update_tables(connection, create_edges_table_sql)
    sc.update_tables(connection, create_nodes_table_sql)
    sc.update_tables(connection, create_scc_table_sql)
    sc.update_tables(connection, create_path_table_sql)
    sc.update_tables(connection, create_delta_path_view_sql)
    sc.update_tables(connection, create_delta_path_view_2_sql)

    # import extensional database
    sc.import_tables_from_csv_directed(connection, "inputs/SCC/" + input_filename)

    sc.update_tables(connection, tgd_1_sql)
    sc.update_tables(connection, tgd_2_sql)

    sc.update_tables(connection, tgd_3_1_sql)
    delta_path = sc.extract_rows(connection, delta_path_sql)

    while len(delta_path) != 0:
        sc.update_tables(connection, tgd_3_2_sql)
        sc.update_tables(connection, tgd_3_3_sql)
        sc.update_tables(connection, tgd_3_4_sql)
        sc.update_tables(connection, tgd_3_5_sql)
        sc.update_tables(connection, tgd_3_6_sql)
        delta_path = sc.extract_rows(connection, delta_path_sql)
        print("finished round")

    equality_graph_edges = sc.extract_rows(connection, egd_sql)
    G = ec.init_equality_graph(equality_graph_edges)
    cc = ec.cc_equality_graph(G)

    scc = sc.extract_rows(connection, scc_view_sql)
    sc.update_tables(connection, scc_view_drop_sql)
    unify_labels(scc, cc, connection)

    # run conjunctive queries
    is_scc = len(sc.extract_rows(connection, bcq_is_scc)) == 0
    print("The graph is strongly connected: " + str(is_scc))

    same_scc = sc.extract_rows(connection, cq_same_scc)
    connection.close()

    with open('outputs/SCC/' + output_filename, 'w', newline='') as csvfile:
        # creating  a csv writer object
        csvwriter = csv.writer(csvfile)
        # writing the data rows
        csvwriter.writerows(same_scc)


if __name__ == "__main__":
    start_time = time.time()
    assert (len(sys.argv) == 3)
    input_filename = sys.argv[1]
    output_filename = sys.argv[2]
    do_chase_and_run_queries_on_disk(input_filename, output_filename)
    print("---SCC in memory execution %s seconds ---" % (time.time() - start_time))
