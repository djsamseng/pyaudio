import bson
# import pymongo
import random
import sqlite3
import sqlite_db

from functools import partial
from mpi4py import MPI

import comm as Comm

rank = MPI.COMM_WORLD.Get_rank()
max_rank = MPI.COMM_WORLD.Get_size()

# mongo_client = pymongo.MongoClient('localhost', 27017)
# db = mongo_client.pyaudio

conn = sqlite3.connect("pyaudio.db")


def main():
    if rank == 0:
        # Command processor
        sqlite_db.create_new_node_network_sqlite(conn)
        Comm.command_handler_loop()
        print("Rank: 0 finished", flush=True)
    else:
        Comm.wait_for_init()

    if rank == 1:
        # Audio listener
        Comm.audio_sender_loop()
        print("Rank 1 finished", flush=True)
    elif rank == 2:
        # Audio receiver - knows how to distrubte
        Comm.audio_receiver_loop()
        print("Rank 2: finished", flush=True)
    elif rank > 2:
        # Worker nodes
        owned_nodes = sqlite_db.init_worker_sqlite(conn, rank, 3, max_rank)
        print("Rank: {0} nodes: {1}".format(rank, list(owned_nodes.keys())[:4]), flush=True)
        Comm.worker_node_loop(owned_nodes)
        print("Rank: {0} finished".format(rank), flush=True)

if __name__ == "__main__":
    main()

