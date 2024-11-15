import time
import zmq
import json
import socket
from io import BytesIO
import constants
import logging

logging.basicConfig(level=logging.INFO)

text_file_list = ['3KB.json', '6KB.json', '10KB.json', '12KB.json', '13KB.json', '15KB.json', '25KB.json', '30KB.json',
                  '40KB.json', '50KB.json', '54KB.json', '63KB.json', '73KB.json', '80KB.json', '89KB.json',
                  '97KB.json']

text_index_array = [12, 0, 0, 9, 6, 4, 5, 0, 3, 6, 3, 1, 13, 0, 9, 15, 1, 3, 4, 7, 6, 14, 12, 6, 9, 11, 2, 6, 6, 0, 11,
                    9, 6,
                    1, 0, 0, 10, 1, 2, 7, 6, 5, 2, 10, 11, 8, 0, 0, 3, 12, 0, 1, 8, 6, 3, 7, 8, 4, 8, 8, 2, 3, 8, 7, 10,
                    6,
                    5, 12, 7, 13, 1, 12, 13, 4, 2, 11, 6, 5, 6, 2, 10, 5, 5, 11, 0, 4, 6, 12, 3, 12, 12, 10, 0, 14, 5,
                    13, 0,
                    0, 6, 0]


def send_init_request_to_database(req_id: str, index: int):
    filename = "text_inferencing_fusion_functions/testing_dataset/" + text_file_list[text_index_array[index]]
    # image_file = open("testing_dataset/650KB.jpg", 'rb')
    text_file = open(filename, 'r')
    my_dict = json.loads(text_file.read())
    text = my_dict["input"]
    logging.info("Putting File {0} ".format(filename))
    num_input_text = 1  # Always sending a single text document

    payload_for_couchdb = {"input_array": [text] * num_input_text, "request_id": req_id, "data_op_type": "init_put",
                           "application": constants.TEXT_INFERENCING, "fusion_depth_index": 1,
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

    filename = "text_inferencing_fusion_functions/testing_dataset/" + text_file_list[text_index_array[index]]

    text_file = open(filename, 'rb')
    kb = len(text_file.read()) / 1024
    print("The text file size ", kb)
    input_size = kb
    total_text_length = input_size * 1

    # input_dimension = [50.0]
    # func_to_qos_dict = {"profanity": {"qos_metric": 100}, "spam": {"qos_metric": 100},
    #                     "text_summarization": {"qos_metric": 600}}

    input_dimension = [input_size]
    print(input_dimension)
    func_to_qos_dict = {"spam": {"qos_metric": 100}, "profanity": {"qos_metric": 100},
                        "text_summarization": {"qos_metric": 250}}

    # application = constants.IMAGE_INFERENCING
    application = constants.TEXT_INFERENCING
    # starting_function = constants.PYNSFW
    starting_function = constants.SPAM
    request_type = "get_fusion_group"

    fusion_request_dict = {"request_type": request_type, "application": application,
                           "starting_function": starting_function,
                           "input_dimension": input_dimension, "qos_requirement": func_to_qos_dict}

    arg_request_dict = {"event_payload": "func_exec", "input_array": [], "input_size": input_size,
                        "total_text_length": total_text_length, "request_id": req_id,
                        "application": constants.TEXT_INFERENCING,
                        "request_type": "execute_function", "fusion_request_dict": fusion_request_dict}

    arg_request_dict = json.dumps(arg_request_dict)
    iaas_socket.send_string(arg_request_dict)
    # result = iaas_socket.recv_string()
    # print(result)
    iaas_socket.close()
    return "Successfully sent"


if __name__ == "__main__":

    req_count = 1
    for i in range(0, 100):
        logging.info(
            "Sending Request {0}, Text size {1}, Num Images 1".format(req_count, text_file_list[text_index_array[i]]))

        send_init_request_to_database(str(req_count), i)
        send_request_to_faas_controller(str(req_count), i)
        req_count = req_count + 1
        time.sleep(5)

        # print(len(image_index_array))
