import bson
# import pymongo
import random
import sqlite3

from bson.objectid import ObjectId
from functools import partial
from mpi4py import MPI

comm = MPI.COMM_WORLD
rank = comm.Get_rank()
max_rank = comm.Get_size()

# mongo_client = pymongo.MongoClient('localhost', 27017)
# db = mongo_client.pyaudio

conn = sqlite3.connect("pyaudio.db")

def create_new_node_network_mongo():
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

def create_new_node_network_sqlite():
    conn.execute("DROP TABLE IF EXISTS nodes")
    conn.execute("DROP TABLE IF EXISTS connections")
    conn.execute('''CREATE TABLE nodes
        (id INT PRIMARY KEY NOT NULL);''')
    conn.execute('''CREATE TABLE connections
        (id INT PRIMARY KEY NOT NULL,
         input_node INT NOT NULL REFERENCES nodes(id),
         output_node INT NOT NULL REFERENCES nodes(id));''')
    conn.commit()
    print("Created sqlite tables", flush=True)
    num_connections_per_node = 10
    num_nodes = 10 * 1000
    new_nodes = []
    for i in range(num_nodes):
        conn.execute('''INSERT INTO nodes
            (id)
            VALUES({0})'''.format(i))
        new_nodes.append({
            "_id": str(i),
            "output_connections": [],
            "input_connections": [],
        })
    conn.commit()
    conn_id = 0
    for i in range(num_nodes):
        for j in range(num_connections_per_node):
            dest = random.randint(0, num_nodes - 1)
            while dest is i:
                dest = random.randint(0, num_nodes - 1)
            conn.execute('''INSERT INTO connections
                (id, input_node, output_node)
                VALUES ({0}, {1}, {2})'''.format(conn_id, i, dest))
            conn_id += 1
    conn.commit()
           
def init_worker_sqlite(worker_number, worker_start_number, worker_end_number):
    num_nodes = conn.execute("SELECT COUNT(id) FROM nodes").fetchall()[0][0]
    owned_nodes = {}
    needed_connections = {}
    num_workers = worker_end_number - worker_start_number
    offset_worker_number = worker_number - worker_start_number
    for i in range(num_nodes):
        if (i % num_workers) == offset_worker_number:
            owned_nodes[i] = {}
    sql_node_ids = "("
    sql_node_ids += ", ".join([int(a) for a in owned_nodes.keys()])
    sql_node_ids += ")"
    res = conn.execute('''SELECT 
        nodes.id, connections.input_node, connections.output_node 
        FROM nodes 
        INNER JOIN connections 
        ON nodes.id = connections.input_node 
        WHERE nodes.id in {0}'''.format(sql_node_ids))
    print("Got nodes: {0}".format(res.fetchall))


def init_worker_mongodb(worker_number, worker_start_number, worker_end_number):
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

def handle_req(req):
    if req.test():
        msg = req.wait()
        if msg and "key" in msg:
            return msg
    return None

def main():
    if rank == 0:
        # Command processor
        create_new_node_network_sqlite()
        for i in range(1, max_rank):
            comm.send({ "key": "init" }, dest=i, tag=11)
        command = None
        while command is not "e":
            command = input("Enter command: ")
            print("Received command: {0}".format(command))
        # comm.isend({"hey": 5}, dest=1).wait()
        print("Exiting", flush=True)
        for i in range(1, max_rank):
            print("Send exit to {0}".format(i), flush=True)
            comm.send({ "key": "exit" }, dest=i)
        print("Rank 0 finished", flush=True)
    else:
        res = comm.recv(source=0, tag=11)
        print("Rank {0} received init: {1}".format(rank, res), flush=True)

    if rank == 1:
        # Audio listener
        import audio
        send_audio_func = partial(comm.isend, dest=2)
        receive_command_func = partial(comm.irecv, source=0)
        audio.record(send_audio_func, receive_command_func)
        print("Rank 1 finished", flush=True)
    elif rank == 2:
        # Audio receiver - knows how to distrubte
        audio_inputs = []
        while True:
            req = comm.irecv(source=MPI.ANY_SOURCE)
            data = handle_req(req)
            if data and data["key"] == "exit":
                break
            if data and data["key"] == "audio_input":
                audio_inputs.append(data["data"])
        print("Got audio data len: {0}".format(len(audio_inputs)), flush=True)

        print("Rank 2 finished", flush=True)
    elif rank == 3:
        init_worker(3, 3, max_rank)
    else:
        # Worker nodes
        pass

if __name__ == "__main__":
    main()

