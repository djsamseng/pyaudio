from mpi4py import MPI

from functools import partial


comm = MPI.COMM_WORLD
rank = comm.Get_rank()
max_rank = comm.Get_size()

def send_input_audio():
    pass

def handle_req(req):
    if req.test():
        msg = req.wait()
        if msg and "key" in msg:
            return msg
    return None

def command_handler_loop():
    for i in range(1, max_rank):
        comm.send({ "key": "init" }, dest=i, tag=11)
    command = None
    while command is not "e":
        command = input("Enter command: ")
        print("Received command: {0}".format(command))
    print("Exiting", flush=True)
    for i in range(1, max_rank):
        print("Send exit to {0}".format(i), flush=True)
        comm.send({ "key": "exit" }, dest=i)

def wait_for_init():
    res = comm.recv(source=0, tag=11)
    print("Rank {0} received init: {1}".format(rank, res), flush=True)
   

def audio_sender_loop():
    import audio
    send_audio_func = partial(comm.isend, dest=2)
    receive_command_func = partial(comm.irecv, source=0)
    audio.record(send_audio_func, receive_command_func)

def audio_receiver_loop():
    audio_inputs = []
    while True:
        req = comm.irecv(source=MPI.ANY_SOURCE)
        data = handle_req(req)
        if data and data["key"] == "exit":
            break
        if data and data["key"] == "audio_input":
            audio_inputs.append(data["data"])
    print("Got audio data len: {0}".format(len(audio_inputs)), flush=True)


def update_inputs(owned_nodes, data):
    pass


def worker_node_loop(owned_nodes):
    while True:
        req = comm.irecv(source=MPI.ANY_SOURCE)
        data = handle_req(req)
        if data and data["key"] == "exit":
            break
        if data and data["key"] == "input_update":
            update_inputs(owned_nodes, data)
