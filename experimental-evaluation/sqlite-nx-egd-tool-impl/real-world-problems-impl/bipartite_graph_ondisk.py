#!/usr/bin/python
import sys
import egd_common as ec
import sqlite_common as sc
import csv
import time

# SQL scripts

create_edges_table_sql = "create table if not exists edges(nodeFrom text, nodeTo text);"
create_nodes_table_sql = "create table if not exists nodes(node text, label text);"
create_partition_table_sql = "create table if not exists partitions(node text, label text, partition text);"

nodes_view_sql = "SELECT * FROM nodes;"

partitions_view_sql = "SELECT * FROM partitions;"

edges_view_sql = "SELECT * FROM edges;"

harmless_egd_sql = "SELECT n1.label, n2.label\
                    FROM edges AS e1, edges AS e2, nodes AS n1, nodes AS n2 \
                    WHERE n1.node=e1.nodeFrom AND n2.node=e2.nodeTo AND e1.nodeTo=e2.nodeFrom;"

bcq_is_bipartite = "SELECT p1.label\
                       FROM partitions AS p1, partitions AS p2, edges AS e\
                       WHERE p1.node=e.nodeFrom AND p2.node=e.nodeTo AND p1.partition=p2.partition"

cq_same_partition = "SELECT p1.node, p2.node\
                       FROM partitions AS p1, partitions AS p2\
                       WHERE p1.partition=p2.partition"


def create_partitions(nodes, cc, connection):
    for part, l in cc.items():
        for n, label in nodes:
            if label in l or label == part:
                cur = connection.cursor()
                cur.execute("insert into " + "partitions" + " values (?,?,?);", (n, label, part))
    connection.commit()


def do_chase_and_run_queries_on_disk(input_filename:str, output_filename:str):
    # create a connection to an in memory database
    connection = sc.create_connection_persistent("app/bipartite.db")

    # create the schema for the predicates
    sc.update_tables(connection, create_edges_table_sql)
    sc.update_tables(connection, create_nodes_table_sql)
    sc.update_tables(connection, create_partition_table_sql)

    # import extensional database
    sc.import_tables_from_csv_undirected_with_labels(connection, "inputs/BipartiteGraph/" + input_filename)

    # apply harmless egds to generate the new predicate
    equality_graph_edges = sc.extract_rows(connection, harmless_egd_sql)
    nodes = sc.extract_rows(connection, nodes_view_sql)
    G = ec.init_equality_graph(equality_graph_edges)
    cc = ec.cc_equality_graph(G)
    create_partitions(nodes, cc, connection)

    # run conjunctive queries
    is_bipartite = len(sc.extract_rows(connection, bcq_is_bipartite)) == 0
    print("The graph is bipartite: " + str(is_bipartite))

    same_partitions_pair = sc.extract_rows(connection, cq_same_partition)
    connection.close()

    with open('outputs/BipartiteGraph/' + output_filename, 'w', newline='') as csvfile:
        # creating  a csv writer object
        csvwriter = csv.writer(csvfile)
        # writing the data rows
        csvwriter.writerows(same_partitions_pair)


if __name__ == "__main__":
    start_time = time.time()
    assert (len(sys.argv) == 3)
    input_filename = sys.argv[1]
    output_filename = sys.argv[2]
    do_chase_and_run_queries_on_disk(input_filename, output_filename)
    print("---Bipartite graph in memory execution %s seconds ---" % (time.time() - start_time))
