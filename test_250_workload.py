import time

import zmq
import json
from PIL import Image
import base64
import socket
from io import BytesIO
import constants
import logging

logging.basicConfig(level=logging.INFO)

image_array = ['63KB.jpg', '86KB.jpg', '94KB.jpg', '150KB.jpg', '220KB.jpg', '336KB.jpg', '400KB.jpg', '500KB.jpg',
               '650KB.jpg', '800KB.jpg', '950KB.jpg', '1400KB.jpg', '1600KB.jpg', '1750KB.jpg', '1950KB.jpg',
               '2100KB.jpg']

num_images_array = [3, 1, 4, 1, 3, 1, 5, 3, 2, 5, 3, 2, 2, 4, 2, 3, 2, 2, 3, 2, 2, 1, 4, 1, 5, 1, 4, 5, 4, 3, 4, 2, 1,
                    1, 1, 4, 3, 2, 3, 4, 2, 4, 3, 5, 2, 2, 1, 2, 4, 3, 1, 4, 5, 4, 3, 2, 5, 3, 1, 4, 3, 1, 5, 1, 3, 1,
                    2, 3, 3, 1, 3, 5, 5, 5, 1, 2, 5, 2, 5, 5, 1, 1, 1, 2, 5, 2, 3, 4, 4, 3, 3, 1, 3, 4, 2, 1, 5, 2, 4,
                    4]
image_index_array = [8, 11, 12, 4, 5, 7, 15, 5, 8, 7, 8, 13, 3, 12, 0, 11, 8, 14, 7, 12, 8, 5, 1, 1, 7, 4, 5, 7, 8, 12,
                     0, 6, 14, 9, 10, 7, 8, 10, 12, 7, 12, 6, 8, 4, 8, 6, 5, 8, 7, 4, 8, 11, 7, 11, 4, 10, 6, 10, 15, 2,
                     7, 0, 5, 7, 7, 9, 8, 2, 4, 6, 8, 2, 6, 13, 12, 13, 1, 4, 4, 10, 6, 9, 6, 11, 7, 9, 8, 7, 11, 7, 3,
                     6, 12, 9, 6, 9, 10, 7, 13, 11]

image_index_array = image_index_array + image_index_array + image_index_array[:50]
num_images_array = num_images_array + num_images_array + num_images_array[:50]
print(len(image_index_array))
print(len(num_images_array))


# exit(0)

def send_init_request_to_database(req_id: str, index: int):
    filename = "testing_dataset/" + image_array[image_index_array[index]]
    # image_file = open("testing_dataset/650KB.jpg", 'rb')
    image_file = open(filename, 'rb')
    img = base64.b64encode(image_file.read()).hex()
    logging.info("Putting File {0} ".format(filename))
    num_input_img = num_images_array[index]

    payload_for_couchdb = {"input_array": [img] * num_input_img, "request_id": req_id, "data_op_type": "init_put",
                           "application": constants.IMAGE_INFERENCING, "fusion_depth_index": 1,
                           "sending_function": "client"}

    '''
    Logic to get the local IP address
    '''
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.connect(('8.8.8.8', 1))  # connect() for UDP doesn't send packets
    local_ip_address = s.getsockname()[0]

    logging.debug("[Fusion Worker] Send the output to the CouchDB server")
    func_context = zmq.Context()
    func_socket = func_context.socket(zmq.REQ)

    # logging.info(container_ip_address, arg_event_payload)
    func_socket.connect("tcp://" + constants.CLOUD_CONTROLLER + ":12000")

    func_socket.send_pyobj(payload_for_couchdb)
    result_from_func = func_socket.recv_pyobj()
    print(result_from_func["result"])
    func_socket.close()

    return "Successfully sent"


def send_request_to_faas_controller(req_id: str, index: int):
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

    filename = "testing_dataset/" + image_array[image_index_array[index]]

    image_file = open(filename, 'rb')
    kb = (len(image_file.read()) / 1024)
    print("The image size before encoding ", kb)
    input_size = kb
    total_image_length = input_size * num_images_array[index]

    with Image.open(filename) as image:
        width, height = image.size

    # input_dimension = [50.0]
    # func_to_qos_dict = {"profanity": {"qos_metric": 100}, "spam": {"qos_metric": 100},
    #                     "text_summarization": {"qos_metric": 600}}

    input_dimension = [kb, width, height, num_images_array[index], kb * num_images_array[index]]
    print(input_dimension)
    func_to_qos_dict = {"nsfw": {"qos_metric": 1500}, "multi_image_resizing": {"qos_metric": 3000},
                        "image_annotation": {"qos_metric": 3000}, "image_concatenation": {"qos_metric": 6000}}

    application = constants.IMAGE_INFERENCING
    # application = constants.TEXT_INFERENCING
    starting_function = constants.PYNSFW
    # starting_function = constants.SPAM
    request_type = "get_fusion_group"

    fusion_request_dict = {"request_type": request_type, "application": application,
                           "starting_function": starting_function,
                           "input_dimension": input_dimension, "qos_requirement": func_to_qos_dict}

    arg_request_dict = {"event_payload": "func_exec", "input_array": [], "input_size": input_size,
                        "total_image_length": total_image_length, "request_id": req_id,
                        "application": "image_inferencing",
                        "request_type": "execute_function", "fusion_request_dict": fusion_request_dict}

    arg_request_dict = json.dumps(arg_request_dict)
    iaas_socket.send_string(arg_request_dict)
    # result = iaas_socket.recv_string()
    # print(result)
    iaas_socket.close()

    return "Successfully sent"


if __name__ == "__main__":

    req_count = 1
    for i in range(0, len(image_index_array)):
        logging.info(
            "Sending Request {0}, Image size {1}, Num Images {2}".format(req_count, image_array[image_index_array[i]],
                                                                         num_images_array[i]))

        send_init_request_to_database(str(req_count), i)
        send_request_to_faas_controller(str(req_count), i)
        req_count = req_count + 1
        time.sleep(15)

    print(len(image_index_array))
