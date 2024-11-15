import zmq
import json
from PIL import Image
import base64
from io import BytesIO

iaas_context = zmq.Context()
iaas_socket = iaas_context.socket(zmq.REQ)
# iaas_socket.connect("tcp://" + "192.168.0.110" + ":15000")

iaas_socket.connect("tcp://" + "172.17.0.3" + ":15000")

with open("Barack_Obama.json", "r") as my_file:
    text = my_file.read()

my_dict = json.loads(text)
text_array = [my_dict["input"]]

arg_request_dict = {"input_array": text_array, "input_size": len(text_array),
                    "total_text_length": len(text) * len(text_array),
                    "request_id": 1}

iaas_socket.send_pyobj(arg_request_dict)
result = iaas_socket.recv_pyobj()
# print(result)
iaas_socket.close()
