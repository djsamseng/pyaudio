import bson
# import pymongo
import random
import sqlite3
import sqlite_db

from functools import partial
from mpi4py import MPI

comm = MPI.COMM_WORLD
rank = comm.Get_rank()
max_rank = comm.Get_size()

# mongo_client = pymongo.MongoClient('localhost', 27017)
# db = mongo_client.pyaudio

conn = sqlite3.connect("pyaudio.db")

def handle_req(req):
    if req.test():
        msg = req.wait()
        if msg and "key" in msg:
            return msg
    return None

def main():
    if rank == 0:
        # Command processor
        sqlite_db.create_new_node_network_sqlite(conn)
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
        sqlite_db.init_worker_sqlite(conn, 3, 3, max_rank)
    else:
        # Worker nodes
        pass

if __name__ == "__main__":
    main()

