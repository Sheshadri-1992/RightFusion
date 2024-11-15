from kafka import KafkaConsumer
import sys
import json
import socket
import subprocess
import zmq
import requests
import logging

logging.basicConfig(level=logging.INFO)


def process_event(event_json: dict):

    logging.info("[Event Manager] Process event {0}".format(event_json["fusion_depth_index"]))
    logging.info("[Event Manager] Resource Dict {0}".format(event_json["resource_dict"]))

    fusion_index = event_json["fusion_depth_index"]
    fusion_depth_list = event_json["fusion_depth_list"]
    if fusion_index <= len(fusion_depth_list)-1:
        fusion_depth = fusion_depth_list[fusion_index]
        """
        All the functions that come out of this need to be called simultaneously
        Also need state maintenance at cloud event manager per request
        """
        logging.info("[Event Manager] Next Fusion group {0}".
                     format(event_json["depth_to_fusion_group_dict"][str(fusion_depth)]))

        '''
        Call only cloud controller, the cloud controller may decide to call Edge/Cloud next
        '''
        fusion_group_dict = event_json["depth_to_fusion_group_dict"]
        fusion_groups_to_execute = fusion_group_dict[str(fusion_depth)]
        logging.info("[Cloud Event Manager] fusion_group_dict {0}".format(fusion_group_dict))
        logging.info("[Cloud Event Manager] event_json {0}".format(event_json["fusion_group"]))
        logging.info("[Cloud Event Manager] Type event json {0}".format(type(event_json)))

        for next_fusion_group in fusion_groups_to_execute:
            logging.info("[Cloud Event Manager] Sending request to fusion group {0}".format(next_fusion_group))
            event_json["fusion_group"] = next_fusion_group
            if len(next_fusion_group) != 0:
                function_in_group = next_fusion_group[0]
                if "xeon" in event_json["resource_dict"][function_in_group]:
                    event_json["layer"] = "xeon"
                else:
                    event_json["layer"] = "raspberry"

            cloud_context = zmq.Context()
            cloud_socket = cloud_context.socket(zmq.PUSH)

            cloud_ip_address = local_ip_address

            # print(container_ip_address, arg_event_payload)
            cloud_socket.connect("tcp://" + cloud_ip_address + ":10000")

            cloud_socket.send_string(json.dumps(event_json))
            cloud_socket.close()



    # if event_json["request"] == "finished":
    #     logging.info("[Event Manager] Send the request to Cloud Controller")


if __name__ == "__main__":

    '''
    Logic to get the local IP address
    '''
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.connect(('8.8.8.8', 1))  # connect() for UDP doesn't send packets
    local_ip_address = s.getsockname()[0]

    packet_holding_dict = {}
    number_of_packets_dict = {}

    # '''
    # Logic to bind to zmq socket
    # '''
    # context = zmq.Context()
    # my_socket = context.socket(zmq.PULL)
    # my_socket.bind("tcp://" + local_ip_address + ":14000")

    consumer = KafkaConsumer('fusion', bootstrap_servers='localhost:9092')
    for msg in consumer:
        logging.info("HERE")
        payload = msg.value.decode()
        # print(payload)
        event_dict = json.loads(payload)
        # print(event_dict)
        process_event(event_dict)