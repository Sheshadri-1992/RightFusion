import zmq
import socket
import requests
import json
import time
import docker
import os
import base64
import logging
from PIL import Image
from io import BytesIO

logging.basicConfig(level=logging.DEBUG)


def convert_json_to_pyobj(arg_event_json):
    logging.info("[Fusion Worker] Convert json to pyobj for image objects")

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


def download_item_from_database(arg_event_payload: dict):
    logging.info("[Fusion Worker] Download the output from the CouchDB server {0}".format(arg_event_payload))
    func_context = zmq.Context()
    func_socket = func_context.socket(zmq.REQ)

    # logging.info(container_ip_address, arg_event_payload)
    func_socket.connect("tcp://" + local_ip_address + ":12000")

    arg_event_payload["data_op_type"] = "get"
    func_socket.send_pyobj(arg_event_payload)
    result_from_func = func_socket.recv_pyobj()
    func_socket.close()

    return result_from_func["input_array"]


class FusionWorker:

    def __init__(self):

        """
        Container related structures
        """

        self.function_ip_dict = {}
        self.container_resource_dict = {}

        '''
        Container lists
        '''
        nsfw_containers = ["rf_pytnsfw1", "rf_pytnsfw2", "rf_pytnsfw3"]  # , "rf_pytnsfw2", "rf_pytnsfw3"]
        image_annotation_containers = ["rf_annotation1"]  # , "rf_annotation2", "rf_annotation3"]
        multi_image_resizing_containers = ["rf_multi_ir1"]  # , "rf_multi_ir2", "rf_multi_ir3"]
        image_concat_containers = ["rf_image_concat1"]  # , "rf_image_concat2", "rf_image_concat3"]
        text_summarization_containers = ["rf_ts1"]
        profanity_containers = ["rf_pd1"]
        spam_det_containers = ["rf_sd1"]

        self.function_name_to_container_dict = {"nsfw": nsfw_containers,
                                                "image_annotation": image_annotation_containers,
                                                "multi_image_resizing": multi_image_resizing_containers,
                                                "image_concatenation": image_concat_containers,
                                                "spam": spam_det_containers,
                                                "profanity": profanity_containers,
                                                "text_summarization": text_summarization_containers}

        self.container_list = ([] + nsfw_containers + image_annotation_containers + multi_image_resizing_containers
                               + image_concat_containers + text_summarization_containers + profanity_containers +
                               spam_det_containers)

        # client = docker.DockerClient()
        self.client = docker.from_env()

        for container_name in self.container_list:
            self.container_resource_dict[container_name] = 1
            try:
                container = self.client.containers.get(container_name)
                container_state = container.attrs['State']['Status']
                if container_state == "exited":
                    logging.info("[Fusion Worker] Cold start. Starting the container {0}".format(container_name))
                    os.system("docker start {0}".format(container_name))

                    if container_name == "rf_pytnsfw2":
                        # os.system("docker update {0} --cpus=2 --cpuset-cpus=13,15".format(container_name))
                        os.system("docker update {0} --cpus=2 --cpuset-cpus=7,9".format(container_name))
                        self.container_resource_dict[container_name] = 2
                    elif container_name == "rf_pytnsfw3":
                        os.system("docker update {0} --cpus=3 --cpuset-cpus=7,9,11".format(container_name))
                        self.container_resource_dict[container_name] = 3
                    else:
                        # os.system("docker update {0} --cpus=1 --cpuset-cpus=7".format(container_name))
                        self.container_resource_dict[container_name] = 1

                    os.system("docker exec {0} /bin/sh -c \"bash /code/start_all_scripts.sh > /dev/null 2>&1\""
                              .format(container_name))
                else:
                    self.container_resource_dict[container_name] = int(
                        container.attrs["HostConfig"]["NanoCpus"] / 1000000000)

                ip_add = container.attrs['NetworkSettings']['IPAddress']
                self.function_ip_dict[container_name] = ip_add
            except docker.errors.NotFound as e:
                logging.info("Container {0} does not exist".format(container_name))
                logging.error("Exception message: {0}".format(e))
            except Exception as e:
                logging.info("Container {0} does not exist".format(container_name))
                logging.error("Exception message: {0}".format(e))

        logging.info("Container IP marking {0}".format(self.function_ip_dict))
        logging.info("Container Resource Dict marking {0} ".format(self.container_resource_dict))

    def select_execution_instance(self, arg_function_name: str, required_resource_spec: dict) -> str:
        func_container_list: list = self.function_name_to_container_dict[arg_function_name]

        index = 0
        found = False
        for container_name in func_container_list:
            resource_layer = "xeon"
            num_cpu = 1
            print("Container name => {0}".format(container_name))
            print(required_resource_spec[arg_function_name])
            print("Container resource spec ", self.container_resource_dict)
            if resource_layer in required_resource_spec[arg_function_name]:
                num_cpu = required_resource_spec[arg_function_name][resource_layer]

            if self.container_resource_dict[container_name] == num_cpu:
                found = True
                break

            index = index + 1

        print("found variable ", found)
        if found is False:
            one_cpuset = "7"
            two_cpuset = "7,9"
            three_cpuset = "7,9,11"
            cpuset = one_cpuset
            if num_cpu == 3:
                cpuset = three_cpuset
            if num_cpu == 2:
                cpuset = two_cpuset

            os.system("docker update {0} --cpus={1} --cpuset-cpus={2}".format(func_container_list[0], num_cpu, cpuset))
            logging.info(
                "docker update {0} --cpus={1} --cpuset-cpus={2}".format(func_container_list[0], num_cpu, cpuset))

            chosen_container = func_container_list[0]
            self.container_resource_dict[chosen_container] = num_cpu
        else:
            chosen_container = func_container_list[index]

        container_info = {"container_name": chosen_container, "num_cpu": num_cpu}
        logging.info("[Fusion Worker] Chosen Worker and resource spec {0}".format(container_info))

        return container_info

    def send_request_to_a_function(self, function_name: str, arg_event_payload: dict, resource_spec: dict):
        func_context = zmq.Context()
        func_socket = func_context.socket(zmq.REQ)

        container_info_dict: dict = self.select_execution_instance(function_name, resource_spec)
        arg_event_payload["num_cpu"] = container_info_dict["num_cpu"]
        container_ip_address = self.function_ip_dict[container_info_dict["container_name"]]

        print(container_ip_address)  # , arg_event_payload)
        func_socket.connect("tcp://" + container_ip_address + ":15000")
        func_socket.send_pyobj(arg_event_payload)
        result_from_func = func_socket.recv_pyobj()

        func_socket.close()

        return arg_event_payload

    def write_output_to_the_data_service(self, arg_event_payload):
        logging.debug("[Fusion Worker] Send the output to the CouchDB server")
        func_context = zmq.Context()
        func_socket = func_context.socket(zmq.REQ)

        # logging.info(container_ip_address, arg_event_payload)
        func_socket.connect("tcp://" + local_ip_address + ":12000")

        func_socket.send_pyobj(arg_event_payload)
        result_from_func = func_socket.recv_pyobj()
        #
        func_socket.close()

    def execute_fusion_group(self, arg_fusion_group: list, arg_function_resource_dict: dict,
                             arg_event_dict: dict) -> json:
        """

        :param arg_fusion_group: The functions in the fusion group
        :param arg_function_resource_dict: Mapping from function name to resource
                                       (resource type is implicit as the request comes to appropriate worker)
        :param arg_event_dict: Contains the image as a payload
        :return: A result json object after processing through the functions in the fusion group
        """

        fusion_depth_index = arg_event_dict["fusion_depth_index"]
        fusion_depth = arg_event_dict["fusion_depth_list"][fusion_depth_index]
        print(fusion_depth, arg_event_dict["depth_to_fusion_group_dict"])

        sent_fusion_group = arg_event_dict["fusion_group"]
        logging.info("[Fusion Worker] Fusion group function list {0}".format(sent_fusion_group))

        """
        Download the data for the fusion group
        """
        arg_event_dict["sending_function"] = sent_fusion_group[0]
        """
        Download the input array from the data service if it is empty
        """

        print(len(arg_event_dict["input_array"]))

        if "input_array" in arg_event_dict:
            if (arg_event_dict["input_array"] is None) or (len(arg_event_dict["input_array"]) == 0):
                arg_event_dict["input_array"] = download_item_from_database(arg_event_dict)

        event_payload = arg_event_dict.copy()
        arg_event_dict = convert_json_to_pyobj(arg_event_dict)

        print("It should be downloaded now", len(arg_event_dict["input_array"]))

        print("Before ", len(arg_event_dict["input_array"]))

        for function in sent_fusion_group:
            print("[Fusion Worker] Function is {0}".format(function))
            result = self.send_request_to_a_function(function, arg_event_dict, arg_function_resource_dict)
            arg_event_dict["input_array"] = result["input_array"]
            # event_payload = result

        arg_event_dict["completed"] = arg_event_dict["fusion_depth_index"]
        arg_event_dict["fusion_depth_index"] = arg_event_dict["fusion_depth_index"] + 1
        arg_event_dict["data_op_type"] = "put"
        arg_event_dict["sending_function"] = function
        arg_event_dict["input_array"] = event_payload["input_array"]

        """
        Forward it to CouchDB server, let it forward to event manager which should figure out what function to call next
        The state management for each request should also be handled at Event Manager
        """
        #arg_event_dict["input_array"] = input_payload
        print("After ", len(arg_event_dict["input_array"]))
        self.write_output_to_the_data_service(arg_event_dict)


if __name__ == "__main__":

    event_dict = {"Hello": "World"}

    fusion_group = ["rf_pytnsfw1", "rf_annotation"]
    fusion_resource_dict = {"rf_pytnsfw1": 1, "rf_annotation": 1}

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
    my_socket.bind("tcp://" + local_ip_address + ":11000")

    print("[Fusion Worker] Service started send requests to tcp://{0}:11000".format(local_ip_address))

    fusion_worker_obj = FusionWorker()

    while True:
        request_dict = my_socket.recv_pyobj()
        print("Resource dict ", request_dict["resource_dict"])
        fusion_worker_obj.execute_fusion_group(fusion_group, request_dict["resource_dict"], request_dict)

        # response_dict = {"result": "Successful"}
        # my_socket.send_pyobj(response_dict)
