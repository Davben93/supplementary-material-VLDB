#!/usr/bin/python

import sqlite3
from sqlite3 import Error
import csv
import hashlib


def create_connection_inmemory():
    """ create a database connection to a database that resides
        in the memory
    """
    conn = None
    try:
        conn = sqlite3.connect(':memory:')
    except Error as e:
        print(e)
    return conn


def create_connection_persistent(db_file):
    """ create a database connection to a SQLite database """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
    except Error as e:
        print(e)
    return conn


def update_tables(conn, sql_script: str):
    try:
        c = conn.cursor()
        c.execute(sql_script)
        conn.commit()
    except Error as e:
        print(e)


def extract_rows(conn, sql_script: str):
    c = conn.cursor()
    c.execute(sql_script)
    return c.fetchall()


def import_tables_from_csv_undirected_with_labels(conn, csv_path):
    with open(csv_path, 'r') as f:
        edges = csv.reader(f)
        cur = conn.cursor()
        nodes = set()
        for edge in edges:
            cur.execute("insert into " + "edges" + " values (?, ?);", edge)
            edge_reversed = (edge[1], edge[0])
            cur.execute("insert into " + "edges" + " values (?, ?);", edge_reversed)
            nodes.add(edge[0])
            nodes.add(edge[1])
        nodes = list(nodes)
        for node in nodes:
            label = str(hashlib.md5(node.encode('utf-8')).hexdigest())
            cur.execute("insert into " + "nodes" + " values (?,?);", (node, label))
        conn.commit()


def import_tables_from_csv_undirected(conn, csv_path):
    with open(csv_path, 'r') as f:
        edges = csv.reader(f)
        cur = conn.cursor()
        nodes = set()
        for edge in edges:
            cur.execute("insert into " + "edges" + " values (?, ?);", edge)
            edge_reversed = (edge[1], edge[0])
            cur.execute("insert into " + "edges" + " values (?, ?);", edge_reversed)
            nodes.add(edge[0])
            nodes.add(edge[1])
        nodes = list(nodes)
        for node in nodes:
            cur.execute("insert into " + "nodes" + " values (?);", (node,))
        conn.commit()


def import_tables_from_csv_directed(conn, csv_path):
    with open(csv_path, 'r') as f:
        edges = csv.reader(f)
        cur = conn.cursor()
        nodes = set()
        for edge in edges:
            cur.execute("insert into " + "edges" + " values (?, ?);", edge)
            nodes.add(edge[0])
            nodes.add(edge[1])
        nodes = list(nodes)
        for node in nodes:
            cur.execute("insert into " + "nodes" + " values (?);", (node,))
        conn.commit()
