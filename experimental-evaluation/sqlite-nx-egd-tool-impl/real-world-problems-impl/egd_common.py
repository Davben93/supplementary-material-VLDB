#!/usr/bin/python

import networkx as nx


def init_equality_graph(edges):
    G = nx.Graph()
    for row in edges:
        G.add_node(row[0])
        G.add_node(row[1])
        if not G.has_edge(row[0], row[1]):
            G.add_edge(row[0], row[1])
    return G


def cc_equality_graph(G):
    cc = [list(c) for c in nx.connected_components(G)]
    update_n = dict()
    for n_list in cc:
        update_n[n_list[0]] = list(n_list[1:])
    return update_n



