from pymongo import MongoClient
from channels import channels as sources
import csv
from datetime import datetime
import json
from dateutil import parser

LAST_DIGIT = 19
client = MongoClient()
database = client.NEWS
news_collection = database.Telegram
desc_collection = database.Description
channels = [channel["id"] for channel in desc_collection.find({})]
channel_description = {item["id"]: item  for item in desc_collection.find({}, {"_id": 0})}
internal_channels = set()
external_channels = set()
external_people = set()
internal_channels_dict = {}
external_channels_dict = {}
external_people_dict = {}


def make_channel_node_ids():
    for k, i in enumerate(sorted(internal_channels), 1):
        internal_channels_dict[i] = k
    for k, i in enumerate(sorted(external_channels), 1):
        external_channels_dict[i] = k
    for k, i in enumerate(sorted(external_people), 1):
        external_people_dict[i] = k


def find_root(channel_id, news_id):
    if channel_id in internal_channels:
        internal_channels.add(channel_id)
    else:
        external_channels.add(channel_id)
    news = news_collection.find_one({"id": news_id, "peer_id.channel_id": channel_id})
    if news:
        source = news["fwd_from"]
        if not source:
            return
        source_type = source.get("from_id", {})
        if not source_type:
            source_type = {"_": None}
        source_type = source_type.get("_", None)
        if source_type == "PeerChannel":
            source_channel_id = source["from_id"]["channel_id"]
            source_news_id = source["channel_post"]
            find_root(source_channel_id, source_news_id)
        elif source_type == "PeerUser":
            user_id = source["from_id"]["user_id"]
            external_people.add(user_id)
        else:
            print(source)
    else:
        pass


def init(data, phases=None):
    if phases:
        new_data = [[] for _ in range(len(phases))]
    else:
        new_data = []
    for news in data:
        source = news["fwd_from"]
        if source is None:
            continue
        channel_id = news["peer_id"]["channel_id"]
        target_phase = channel_description[channel_id]["phase"]
        if phases and target_phase not in phases:
            continue
        if phases:
            new_data[target_phase-1].append(news)
        internal_channels.add(channel_id)
        source_type = source.get("from_id", {})
        if not source_type:
            source_type = {"_": None}
        source_type = source_type.get("_", None)
        if source_type == "PeerChannel":
            source_channel_id = source["from_id"]["channel_id"]
            if phases:
                if source_channel_id in channels and channel_description[source_channel_id]["phase"] in phases:
                    internal_channels.add(source_channel_id)
                else:
                    external_channels.add(source_channel_id)
            else:
                external_channels.add(source_channel_id)
            # find_root(source_channel_id, source_news_id)
        elif source_type == "PeerUser":
            user_id = source["from_id"]["user_id"]
            external_people.add(user_id)
        else:
            name = source["from_name"]
            external_people.add(name)
    return new_data


def get_entity(source):
    source_channel_id = -1
    source_type = source.get("from_id", {})
    if not source_type:
        source_type = {"_": None}
    source_type = source_type.get("_", None)
    if source_type == "PeerChannel":
        source_channel_id = source["from_id"]["channel_id"]
        if source_channel_id in internal_channels_dict:
            s_node = {
                "Id": f"I-{internal_channels_dict[source_channel_id]}",
                "Label": source_channel_id,
                "Name": channel_description[source_channel_id]["name"]
            }
        else:
            s_node = {
                "Id": f"E-{external_channels_dict[source_channel_id]}",
                "Label": source_channel_id,
            }
    elif source_type == "PeerUser":
        user_id = source["from_id"]["user_id"]
        s_node = {
            "Id": f"EP-{external_people_dict[user_id]}",
            "Label": user_id
        }
    else:
        name = source["from_name"]
        s_node = {
            "Id": f"EP-{external_people_dict[name]}",
            "Label": name
        }
    s_node_date = source["date"]
    if isinstance(s_node_date, str):
        s_node_date = s_node_date.replace("T", " ")[:LAST_DIGIT]
        s_node_date = datetime.strptime(s_node_date, "%Y-%m-%d %H:%M:%S")
    s_node["Date"] = s_node_date
    if "Name" not in s_node:
        s_node["Name"] = ""
    return s_node, source_channel_id


def data_to_graph(data, phases=True):
    nodes = {}
    nodes_id_set = set()
    edges = []
    for phase in phases:
        for news in data[phase-1]:
            source = news["fwd_from"]
            if source is None:
                continue
            target_channel_id = news["peer_id"]["channel_id"]
            target_node_id = f"I-{internal_channels_dict[target_channel_id]}"
            target_date = news["date"]
            if isinstance(target_date, str):
                target_date = target_date.replace("T", " ")[:LAST_DIGIT]
                target_date = datetime.strptime(target_date, "%Y-%m-%d %H:%M:%S")
            if target_node_id not in nodes_id_set:
                node = {
                    "Id": f"{target_node_id}",
                    "Label": target_channel_id,
                    "Name": channel_description[target_channel_id]["name"],
                    "Phase": {phase},
                    "Date": target_date
                }
                nodes[target_node_id] = node
                nodes_id_set.add(target_node_id)
            else:
                node = nodes[target_node_id]
                if phase not in node["Phase"] and node["Id"][0] != "I":
                    nodes[target_node_id]["Phase"].add(phase)
                if target_date < nodes[target_node_id]["Date"]:
                    nodes[target_node_id]["Date"] = target_date
            s_node, source_channel_id = get_entity(source)
            s_node_id = s_node["Id"]
            if s_node_id[0] == "I":
                s_node["Phase"] = {channel_description[source_channel_id]["phase"]}
            else:
                s_node["Phase"] = {phase}
            if s_node_id not in nodes_id_set:
                nodes[s_node_id] = s_node
                nodes_id_set.add(s_node_id)
            else:
                source_date = s_node["Date"]
                node = nodes[s_node_id]
                if phase not in node["Phase"] and node["Id"][0] != "I":
                    nodes[s_node_id]["Phase"].add(phase)
                if source_date < node["Date"]:
                    nodes[s_node_id]["Date"] = source_date
            link = {
                "Source": target_node_id,
                "Target": s_node_id,
                "Type": "Directed",
                "Weight": 1,
                "Date": target_date
            }
            if link not in edges and source_channel_id != target_channel_id:
                edges.append(link)

    nodes = [item for item in nodes.values()]
    return nodes, edges

def write_to_csv(filepath, data, headers):
    with open(filepath, "w", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=headers)
        writer.writeheader()
        for node in data:
            writer.writerow(node)

def write_data(data):
    all_data = []
    for item in data:
        message = item["message"]
        if message:
            all_data.append({
              "message": message
            })
    with open("output.json", "w", encoding="utf-8") as f_out:
        json.dump(all_data, f_out, indent=2, ensure_ascii=False)

def make_graph(phases):
    news_with_date = news_collection.find({
        # "fwd_from.from_id": {"$exists": True},
        "message": {"$exists": True},
        "date": {"$gte": datetime(year=2022, month=1, day=1)}
    })
    data = [i for i in news_with_date]
    write_data(data)
    exit(-1)
    transformed_data = init(data, phases)
    make_channel_node_ids()
    nodes, edges = data_to_graph(transformed_data, phases)
    for node in nodes:
        if node["Id"][0] == "I":
            node["Phase"] = channel_description[node["Label"]]["phase"]
    write_to_csv("Nodes.csv", nodes, ["Id", "Label", "Name", "Date", "Phase"])
    write_to_csv("Edges.csv", edges, ["Source", "Target", "Type", "Weight", "Date"])


if __name__ == '__main__':
    make_graph([1, 2])
