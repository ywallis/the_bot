import zmq
import time

def heartbeat_sender(message: str):
    context = zmq.Context()
    socket = context.socket(zmq.PUB)
    socket.bind("tcp://*:5558")

    while True:
        socket.send_string(message)
        print(message)
        time.sleep(1)  # Send heartbeat every 1 second

def heartbeat_receiver(stop_event, message_str):
    context = zmq.Context()
    socket = context.socket(zmq.SUB)
    socket.connect("tcp://localhost:5558")
    socket.setsockopt_string(zmq.SUBSCRIBE, "")

    heartbeat_timeout = 2  # Timeout in seconds
    last_heartbeat = time.time()

    while not stop_event.is_set():
        try:
            if socket.poll(heartbeat_timeout * 1000):  # Timeout in milliseconds
                message = socket.recv_string()
                if message == message_str:
                    print("Heartbeat received")
                    last_heartbeat = time.time()
            else:
                # No heartbeat received in the expected time
                if time.time() - last_heartbeat > heartbeat_timeout:
                    print("No heartbeat received, stopping main activity")
                    stop_event.set()  # Signal the main thread to stop
                    break
        except KeyboardInterrupt:
            print("Receiver interrupted")
            break