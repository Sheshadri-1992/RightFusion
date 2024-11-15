import zmq
import socket
import requests
import json
import time
import docker
import os
import base64
from io import BytesIO
from PIL import Image
import time
import logging

import constants

logging.basicConfig(level=logging.DEBUG)

'''
Edge Server is the Edge Controller
'''

def download_item_from_database(arg_event_payload: dict):
    logging.info("[Edge Server] Download the output from the GCP Cloud Emulator Service")
    func_context = zmq.Context()
    func_socket = func_context.socket(zmq.REQ)

    # logging.info(container_ip_address, arg_event_payload)
    func_socket.connect("tcp://" + constants.CLOUD_CONTROLLER + ":12000")

    arg_event_payload["data_op_type"] = "get"
    func_socket.send_pyobj(arg_event_payload)
    result_from_func = func_socket.recv_pyobj()
    func_socket.close()

    return result_from_func["input_array"]

def convert_json_to_pyobj(arg_event_json):
    logging.info("[Edge Server] We are here")

    application = arg_event_json["application"]
    if application == "text_inferencing":
        return arg_event_json

    image_array = arg_event_json["input_array"]
    pil_image_array = []
    for image in image_array:
        byte_array = bytes.fromhex(image)
        byte_array = base64.b64decode(byte_array)
        im = Image.open(BytesIO(byte_array))
        im.save("json_to_bin_image.jpg", 'JPEG')
        pil_image_array.append(im)

    '''
    JSON string 
    '''
    arg_event_json["input_array"] = pil_image_array
    return arg_event_json

def send_request_to_cloud_controller(arg_event_payload):
    arg_event_payload = json.loads(arg_event_payload)
    arg_event_payload["data_op_type"] = "get"
    arg_event_payload["sending_function"] = "edge_server"
    arg_event_payload["input_array"] = download_item_from_database(arg_event_payload)
    arg_event_payload = json.dumps(arg_event_payload)

    cloud_edge_context = zmq.Context()
    cloud_edge_socket = cloud_edge_context.socket(zmq.PUSH)
    cloud_edge_socket.connect("tcp://" + constants.CLOUD_CONTROLLER + ":10000")

    cloud_edge_socket.send_string(arg_event_payload)
    cloud_edge_socket.close()

def call_fusion_service(arg_event_payload):
    """
    Do a zmq push , the fusion worker should pull
    :param arg_event_payload:
    :return:
    """

    container_ip_address = local_ip_address
    # arg_event_payload = convert_json_to_pyobj(arg_event_payload)

    # os.system("docker update func1 --cpus = 3 --cpuset-cpus = 0, 3, 5")

    func_context = zmq.Context()
    func_socket = func_context.socket(zmq.PUSH)

    # logging.info(container_ip_address, arg_event_payload)
    func_socket.connect("tcp://" + constants.RASP_PI4_WORKER1 + ":11000")

    func_socket.send_pyobj(arg_event_payload)
    # result_from_func = func_socket.recv_pyobj()

    func_socket.close()

    return arg_event_payload


if __name__ == "__main__":
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
    my_socket = context.socket(zmq.PULL)
    my_socket.bind("tcp://" + local_ip_address + ":10001")

    logging.info("[Edge Server] Service started send requests to tcp://{0}:10001".format(local_ip_address))

    while True:
        request_dict = my_socket.recv_string()
        exec_payload = request_dict
        request_dict = json.loads(request_dict)
        request_type = request_dict["request_type"]

        if request_type == "execute_function":
            if "layer" in request_dict:
                logging.info("[Edge Server] The layer to which request is sent is {0}".format(request_dict["layer"]))
                if request_dict["layer"] == constants.XEON:
                    logging.info("[Edge Server] Forwarding request to Cloud Server")
                    send_request_to_cloud_controller(exec_payload)
                else:
                    call_fusion_service(request_dict)

        # response_dict = {"result": "Successful"}
        # response_dict = json.dumps(response_dict)
        # my_socket.send_string(response_dict)
