import zmq
import socket
import requests
import json
import time
import logging

import constants

logging.basicConfig(level=logging.DEBUG)


def download_item_from_database(arg_event_payload: dict):
    logging.debug("[Fusion Worker] Download the output from the GCP Cloud server")
    func_context = zmq.Context()
    func_socket = func_context.socket(zmq.REQ)

    # logging.info(container_ip_address, arg_event_payload)
    func_socket.connect("tcp://" + local_ip_address + ":12000")

    arg_event_payload["data_op_type"] = "get"
    func_socket.send_pyobj(arg_event_payload)
    result_from_func = func_socket.recv_pyobj()
    func_socket.close()

    return result_from_func["input_array"]


def retrieve_fusion_groups(arg_fusion_dict_payload: dict):
    """
    Retrieves Fusion groups for the incoming graph
    :param arg_fusion_dict_payload: Contains the application graph, qos metric constraints
    :return: Returns the fusion group and least resource specification
    """
    fusion_context = zmq.Context()
    fusion_socket = fusion_context.socket(zmq.REQ)
    fusion_socket.connect("tcp://" + local_ip_address + ":13000")

    fusion_socket.send_pyobj(arg_fusion_dict_payload)
    response: dict = fusion_socket.recv_pyobj()

    fusion_depth_list = list(response["fusion_group_dict"].keys())
    fusion_depth_list.sort()

    layer = "xeon"
    starting_func = arg_fusion_dict_payload["starting_function"]
    resource_of_starting_func = response["resource_dict"][starting_func]

    print("Starting Func ",resource_of_starting_func)

    if "xeon" in resource_of_starting_func:
        layer = "xeon"
    elif "raspberry" in resource_of_starting_func:
        layer = "raspberry"

    print(layer)

    # print(response)
    response = {"depth_to_fusion_group_dict": response["fusion_group_dict"],
                "resource_dict": response["resource_dict"],
                "fusion_depth_list": fusion_depth_list, "fusion_depth_index": 0,
                "fusion_group": response["fusion_group_dict"][1][0], "layer": layer}

    print("Response from fusion algorithm", response)
    return response


def send_req_to_edge_cloud_controller(arg_request_payload: dict):
    """

    :param arg_request_payload:
    :return:
    """

    """
    Download data for the starting function
    """
    arg_request_payload["data_op_type"] = "get"
    arg_request_payload["sending_function"] = "faas_controller"
    arg_request_payload["input_array"] = download_item_from_database(arg_request_payload)

    layer = constants.XEON
    if "layer" in arg_request_payload:
        layer = arg_request_payload["layer"]

    controller_ip = constants.CLOUD_CONTROLLER
    if layer == constants.RASPBERRY:
        controller_ip = constants.EDGE_CONTROLLER

    logging.info("[FaaS Controller] The chosen layer is {0}, the IP of the chosen layer {1}".format(layer, controller_ip))
    cloud_edge_context = zmq.Context()
    cloud_edge_socket = cloud_edge_context.socket(zmq.PUSH)
    cloud_edge_socket.connect("tcp://" + local_ip_address + ":10000")

    cloud_edge_socket.send_string(json.dumps(arg_request_payload))
    cloud_edge_socket.close()


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
    my_socket.bind("tcp://" + local_ip_address + ":9000")

    logging.info("[Main FaaS Controller] Service started send requests to tcp://{0}:9000".format(local_ip_address))

    while True:
        request_dict = my_socket.recv_string()
        request_dict = json.loads(request_dict)
        request_type = request_dict["request_type"]

        if request_type == "execute_function":
            fusion_dict_payload = request_dict["fusion_request_dict"]

            '''
            Send a request to Fusion Algorithm to retrieve fusion groups
            '''
            start_time = time.time()
            result_fusion_info_dict = retrieve_fusion_groups(fusion_dict_payload)
            for key in result_fusion_info_dict.keys():
                request_dict[key] = result_fusion_info_dict[key]
            end_time = time.time()

            with open("rf_image_inf_strict_fusion.txt", 'a') as fusion_file:
                fusion_file.write(json.dumps(result_fusion_info_dict))
                fusion_file.write("\n")

            print("Request Payload keys {0}".format(request_dict.keys()))
            print("Total fusion_time {0}".format((end_time - start_time)*1000))

            '''
            Send the Request to Edge/Cloud Controller to execute the functions
            '''
            print(request_dict["resource_dict"])
            #send_req_to_edge_cloud_controller(request_dict)
