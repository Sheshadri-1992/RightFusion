import zmq
import json
from PIL import Image
import base64
import socket
from io import BytesIO

import constants

iaas_context = zmq.Context()
iaas_socket = iaas_context.socket(zmq.PUSH)
# iaas_socket.connect("tcp://" + "192.168.0.110" + ":10000")

'''
Logic to get the local IP address
'''
s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
s.connect(('8.8.8.8', 1))  # connect() for UDP doesn't send packets
local_ip_address = s.getsockname()[0]

iaas_socket.connect("tcp://" + constants.CLOUD_CONTROLLER + ":9000")

# image_file = open("56KB.jpg", 'rb')
# img = base64.b64encode(image_file.read()).hex()
# input_size = len(img) / 1024

image_file = open("1400KB.jpg", 'rb')
kb = (len(image_file.read()) / 1024)
print("The image size before encoding ", kb)
input_size = kb
total_image_length = input_size * 5

with Image.open("1400KB.jpg") as image:
    width, height = image.size

# input_dimension = [50.0]
# func_to_qos_dict = {"profanity": {"qos_metric": 100}, "spam": {"qos_metric": 100},
#                     "text_summarization": {"qos_metric": 600}}

input_dimension = [kb, width, height, 5, kb*5]
print(input_dimension)
func_to_qos_dict = {"nsfw": {"qos_metric": 900}, "multi_image_resizing": {"qos_metric": 1500},
                        "image_annotation": {"qos_metric": 1500}, "image_concatenation": {"qos_metric": 1500}}

application = constants.IMAGE_INFERENCING
# application = constants.TEXT_INFERENCING
starting_function = constants.PYNSFW
# starting_function = constants.SPAM
request_type = "get_fusion_group"

fusion_request_dict = {"request_type": request_type, "application": application, "starting_function": starting_function,
                       "input_dimension": input_dimension, "qos_requirement": func_to_qos_dict}

arg_request_dict = {"event_payload": "func_exec", "input_array": [], "input_size": input_size,
                    "total_image_length": total_image_length, "request_id": "21", "application": "image_inferencing",
                    "request_type": "execute_function", "fusion_request_dict": fusion_request_dict}

"""
,
                    "depth_to_fusion_group_dict": fusion_group_dict, "resource_dict": least_resource_dict,
                    "fusion_depth_list": fusion_depth_list, "fusion_depth_index": 0,
                    "fusion_group": fusion_group_dict[1][0]
"""

arg_request_dict = json.dumps(arg_request_dict)
iaas_socket.send_string(arg_request_dict)
# result = iaas_socket.recv_string()
# print(result)
iaas_socket.close()
