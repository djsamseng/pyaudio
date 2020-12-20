import random

def create_new_node_network_mongo(db):
    db.nodes.drop()
    db.connections.drop()
    num_connections_per_node = 10
    num_nodes = 10 * 1000
    new_nodes = []
    for i in range(num_nodes):
        new_nodes.append({
            "_id": str(i),
            "output_connections": [],
            "input_connections": [],
        })
    new_connections = []
    for i in range(num_nodes):
        for j in range(num_connections_per_node):
            dest = random.randint(0, num_nodes - 1)
            while dest is i:
                dest = random.randint(0, num_nodes - 1)
            new_connection = {
                "_id": str(len(new_connections)),
                "input_node": str(i),
                "output_node": str(dest)
            };
            new_nodes[i]["output_connections"].append(new_connection["_id"])
            new_nodes[dest]["input_connections"].append(new_connection["_id"])
            new_connections.append(new_connection)

    res = db.nodes.insert_many(new_nodes)
    print("Inserted nodes: {0}".format(len(res.inserted_ids)))
    res = db.connections.insert_many(new_connections)
    print("Inserted connections: {0}".format(len(res.inserted_ids)))

def init_worker_mongodb(db, worker_number, worker_start_number, worker_end_number):
    num_nodes = db.nodes.count_documents({})
    owned_nodes = {}
    needed_connections = {}
    num_workers = worker_end_number - worker_start_number
    offset_worker_number = worker_number - worker_start_number
    for i in range(num_nodes):
        if (i % num_workers) == offset_worker_number:
            owned_nodes[str(i)] = {}
    for i in owned_nodes.keys():
        node = db.nodes.find_one({ "_id": str(i) })
        owned_nodes[i] = node
        for conn_id in node["input_connections"]:
            needed_connections[conn_id] = {}
        for conn_id in node["output_connections"]:
            needed_connections[conn_id] = {}
    for i in needed_connections.keys():
        connection = db.connections.find_one({ "_id": i })
        needed_connections[i] = connection
    print("Got owned nodes {0}".format(len(owned_nodes.keys())), flush=True)
    print("Got connections {0}".format(len(needed_connections.keys())), flush=True)


