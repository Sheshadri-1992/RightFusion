import zmq
import socket
import json
from PIL import Image
import base64
from io import BytesIO

import constants

'''
Logic to get the local IP address
'''
s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
s.connect(('8.8.8.8', 1))  # connect() for UDP doesn't send packets
local_ip_address = s.getsockname()[0]

'''
ZMQ related code
'''
context = zmq.Context()
my_socket = context.socket(zmq.REQ)
my_socket.connect("tcp://" + local_ip_address + ":13000")

# input_dimension = [1870.0, 2]
# func_to_qos_dict = {"nsfw": {"qos_metric": 1500}, "multi_image_resizing": {"qos_metric": 1500},
#                         "image_annotation": {"qos_metric": 1500}, "image_concatenation": {"qos_metric": 1500}}

input_dimension = [50.0]
func_to_qos_dict = {"profanity": {"qos_metric": 100}, "spam": {"qos_metric": 100},
                        "text_summarization": {"qos_metric": 600}}

# application = constants.IMAGE_INFERENCING
application = constants.TEXT_INFERENCING
# starting_function = constants.PYNSFW
starting_function = constants.SPAM
request_type = "get_fusion_group"

request_dict = {"request_type": request_type, "application": application, "starting_function": starting_function,
                "input_dimension": input_dimension, "qos_requirement": func_to_qos_dict}

my_socket.send_pyobj(request_dict)
result = my_socket.recv_pyobj()
print(result)

