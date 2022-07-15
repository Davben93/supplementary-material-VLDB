#!/usr/bin/python

import sys
import egd_common as ec
import sqlite_common as sc
import csv
import time

# SQL scripts

create_edges_table_sql = "create table if not exists edges(nodeFrom text, nodeTo text);"
create_nodes_table_sql = "create table if not exists nodes(node text);"
create_clique_table_sql = "create table if not exists clique(node1 text, node2 text, node3 text, label text);"
create_cluster_table_sql = "create table if not exists cluster(node text, label text);"

nodes_view_sql = "SELECT * FROM nodes;"
edges_view_sql = "SELECT * FROM edges;"
clique_view_sql = "SELECT * FROM clique;"
cluster_view_sql = "SELECT * FROM cluster;"

# edge(X,Y), edge(Y,Z), edge(Z,X) -> Clique(X,Y,Z,C).

tgd_1_sql = "INSERT INTO clique \
             SELECT e1.nodeFrom, e2.nodeFrom, e3.nodeFrom, \'zXXX\' || e1.nodeFrom || e2.nodeFrom || e3.nodeFrom \
             FROM edges AS e1, edges AS e2, edges AS e3 \
             WHERE e1.nodeTo=e2.nodeFrom AND e2.nodeTo=e3.nodeFrom AND e3.nodeTo=e1.nodeFrom " \
            "AND e1.nodeFrom <e3.nodeFrom;"

# Clique(X,Y,Z,C) -> Cluster(X,C).

tgd_2_sql = "INSERT INTO cluster \
             SELECT c.node1, c.label \
             FROM clique AS c; "

# Clique(X,Y,Z,C) -> Cluster(Y,C).

tgd_3_sql = "INSERT INTO cluster \
             SELECT c.node2, c.label \
             FROM clique AS c; "

# Clique(X,Y,Z,C) -> Cluster(Z,C).

tgd_4_sql = "INSERT INTO cluster \
             SELECT c.node3, c.label \
             FROM clique AS c; "

egd_1_sql = "SELECT DISTINCT c1.label, c2.label\
             FROM clique AS c1, clique AS c2\
             WHERE c1.node1=c2.node1 AND c1.node2=c2.node2;"

egd_2_sql = "SELECT DISTINCT c1.label, c2.label\
             FROM clique AS c1, clique AS c2\
             WHERE c1.node2=c2.node2 AND c1.node3=c2.node3;"

egd_3_sql = "SELECT DISTINCT c1.label, c2.label\
             FROM clique AS c1, clique AS c2\
             WHERE c1.node1=c2.node1 AND c1.node3=c2.node3;"

cq_same_community = "SELECT DISTINCT c1.node, c2.node \
                     FROM cluster AS c1, cluster AS c2 \
                     WHERE c1.label = c2.label;"

cluster_view_drop_sql = "DELETE FROM cluster;"


def unify_labels(cluster, cc, connection):
    for part, l in cc.items():
        for n, label in cluster:
            if label in l or label == part:
                cur = connection.cursor()
                cur.execute("insert into " + "cluster" + " values (?,?);", (n, part))
    connection.commit()


def do_chase_and_run_queries_in_memory(input_filename: str, output_filename: str):
    # create a connection to an in memory database
    connection = sc.create_connection_inmemory()

    # create the schemas for the predicates
    sc.update_tables(connection, create_edges_table_sql)
    sc.update_tables(connection, create_nodes_table_sql)
    sc.update_tables(connection, create_clique_table_sql)
    sc.update_tables(connection, create_cluster_table_sql)

    # import extensional database
    sc.import_tables_from_csv_undirected(connection, "inputs/Clique/" + input_filename)

    sc.update_tables(connection, tgd_1_sql)
    sc.update_tables(connection, tgd_2_sql)
    sc.update_tables(connection, tgd_3_sql)
    sc.update_tables(connection, tgd_4_sql)

    print("TGDs finished.")

    equality_graph_edges_1 = sc.extract_rows(connection, egd_1_sql)
    equality_graph_edges_2 = sc.extract_rows(connection, egd_2_sql)
    equality_graph_edges_3 = sc.extract_rows(connection, egd_3_sql)
    equality_graph_edges = list(set(equality_graph_edges_1 + equality_graph_edges_2 + equality_graph_edges_3))

    G = ec.init_equality_graph(equality_graph_edges)

    print("Equality Graph construction.")

    cc = ec.cc_equality_graph(G)

    print("CC of equality graph finished.")

    cluster = sc.extract_rows(connection, cluster_view_sql)
    sc.update_tables(connection, cluster_view_drop_sql)
    unify_labels(cluster, cc, connection)
    print(cluster)
    print("EGDs unification finished.")

    same_community = sc.extract_rows(connection, cq_same_community)
    connection.close()

    print("Running CQ finished.")

    with open('outputs/Clique/' + output_filename, 'w', newline='') as csvfile:
        # creating  a csv writer object
        csvwriter = csv.writer(csvfile)
        # writing the data rows
        csvwriter.writerows(same_community)


if __name__ == "__main__":
    start_time = time.time()
    assert (len(sys.argv) == 3)
    input_filename = sys.argv[1]
    output_filename = sys.argv[2]
    do_chase_and_run_queries_in_memory(input_filename, output_filename)
    print("---Clique Percolation in memory execution %s seconds ---" % (time.time() - start_time))
