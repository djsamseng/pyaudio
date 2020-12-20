import pyaudio
import time

CHUNK = 1024
FORMAT = pyaudio.paInt16
CHANNELS = 1
RATE = 44100
RECORD_SECONDS = 0.1

# Each element has len(2048) of which each is a number from 0 to 255
global_vars = {}
def callback(in_data, frame_count, time_info, flag):
    global_vars["send_audio_func"]({
        "key": "audio_input",
        "data": in_data
    })
    return in_data, pyaudio.paContinue

def record(send_audio_func, receive_command_func):
    global global_vars
    global_vars["send_audio_func"] = send_audio_func
    p = pyaudio.PyAudio()

    stream = p.open(format=FORMAT,
        channels=CHANNELS,
        rate=RATE,
        input=True,
        frames_per_buffer=CHUNK,
        stream_callback=callback)
    print("Start recording", flush=True)
    stream.start_stream()

    while stream.is_active():
        recv = receive_command_func()
        if (recv.test()):
            msg = recv.wait()
            if msg and "key" in msg:
                if msg["key"] == "exit":
                    stream.stop_stream()
            else:
                print("Got bad msg: {0}".format(msg), flush=True)
                stream.stop_stream()
    print("Done recording", flush=True)

    stream.close()
    p.terminate()

