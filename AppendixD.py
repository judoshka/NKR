from sync_database import spread_collection, query, news_collection
from dateutil import parser
import csv
import os.path
from statistics import mean

def fix_date(date_str):
    if isinstance(date_str, str):
        return parser.parse(date_str)
    return date_str


def build_graph(record, node_id, nodes, edges, dates, parent=0):
    children = record["to"]
    if parent != node_id[0]:
        edges.append((parent, node_id[0]))
        parent = node_id[0]
    for child in children:
        node_id[0] += 1
        nodes.append(node_id[0])
        dates[node_id[0]] = fix_date(child["date"])
        build_graph(child, node_id, nodes, edges, dates, parent)


def distance(a, b):
    n, m = len(a), len(b)
    if n > m:
        a, b = b, a
        n, m = m, n
    current_row = range(n + 1)  # 0 ряд - просто восходящая последовательность (одни вставки)
    for i in range(1, m + 1):
        previous_row, current_row = current_row, [i] + [0] * n
        for j in range(1, n + 1):
            add, delete, change = previous_row[j] + 1, current_row[j - 1] + 1, previous_row[j - 1]
            if a[j - 1] != b[i - 1]:
                change += 1
            current_row[j] = min(add, delete, change)
    return current_row[n]


def write_to_csv(filepath, data, headers):
    with open(filepath, "w", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=headers)
        writer.writeheader()
        for node in data:
            writer.writerow(node)


def build_graph_from_edges(edges):
    graph = {}
    for edge in edges:
        start = edge[0]
        end = edge[1]
        if start not in graph:
            graph[start] = []
        graph[start].append(end)

    return graph


def graph_to_string(graph, node):
    child_cnt = len(graph.get(node, []))
    if not child_cnt:
        return "0"
    else:
        s = []
        for child in graph.get(node, []):
            s.append(graph_to_string(graph, child))
        return str(child_cnt) + "".join(sorted(s))


def create_csv(graph, nodes_count, index, dates, date_weight=False):
    start = 0
    queue = [start]
    d = {i: 0 for i in range(nodes_count)}
    mark = {i: 0 for i in range(nodes_count)}
    d[start] = 0
    mark[start] = 1
    depth = {start: 0}
    nodes = [{"Id": start, 'Label': start, 'Depth': 0, "Mark": 0}]
    edges = []
    x = sorted(dates.values())
    if len(x) > 10:
        min_date_diff = int((x[-5] - x[0]).total_seconds() // 3600)
    elif len(x) > 5:
        min_date_diff = int((x[-3] - x[0]).total_seconds() // 3600)
    else:
        min_date_diff = 0
    while queue:
        value = queue.pop(0)
        for neighbour in graph.get(value, []):
            if mark[neighbour] == 0:
                if date_weight:
                    d[neighbour] = (dates[neighbour] - dates[value]).seconds // 60
                else:
                    d[neighbour] = d[value] + 1
                depth[neighbour] = depth[value] + 1
                d_range = int((dates[neighbour] - dates[0]).total_seconds() // 3600)
                if d_range < min_date_diff:
                    d_range = None
                nodes.append({
                    "Id": neighbour,
                    "Label": neighbour,
                    "Depth": depth[neighbour],
                    "Mark": d_range
                })
                edges.append({
                    "Source": value,
                    "Target": neighbour,
                    "Type": "Directed",
                    "Label": d[neighbour],
                    "Weight": 10 / (d[neighbour] + 1)
                })
                mark[neighbour] = 1
                queue.append(neighbour)
    write_to_csv(os.path.join("spread_src_1", f"Nodes_{index}.csv"), nodes, ["Id", "Label", "Depth", "Mark"])
    write_to_csv(os.path.join("spread_src_1", f"Edges_{index}.csv"), edges, ["Source", "Target", "Type", "Label", "Weight"])


if __name__ == '__main__':
    data = query(spread_collection, {}, {"_id": 0})
    graphs = []
    gr = []
    for index, item in enumerate(data, 0):
        nodes = [0]
        edges = []
        dates = {0: fix_date(item["date"])}
        build_graph(item, [0], nodes, edges, dates, 0)
        graph = build_graph_from_edges(edges)
        if graph and len(nodes) > 100:
            print(index, item)
            print(graph)
        if graph:
            # print(graph)
            # create_csv(graph, len(nodes), index, dates, date_weight=True)
            # continue
            graph_str = graph_to_string(graph, 0)
            graphs.append(graph_str)
            gr.append(len(nodes))
    exit(0)
    count = 0
    values = []
    for i in range(len(graphs) - 1):
        for j in range(i+1, len(graphs)):
            d = distance(graphs[i], graphs[j])
            if d < max([len(graphs[i]), len(graphs[j])]) // 4 and min([len(graphs[i]), len(graphs[j])]) > 10:
                count += 1
                values += [gr[i], gr[j]]
                print(graphs[i], graphs[j], d, gr[i])

    print(count, mean(values))
