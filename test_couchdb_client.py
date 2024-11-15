import json
import logging
import couchdb
import zmq
import socket
import docker
import base64

import constants

logging.basicConfig(level=logging.INFO)

client = docker.from_env()
container = client.containers.get("couchdb")
couchdb_ip_add = container.attrs['NetworkSettings']['IPAddress']

# server = couchdb.Server('http://admin:password@' + couchdb_ip_add + ':5984/')
# try:
#     db = server.create('cloud-couchdb')
# except couchdb.PreconditionFailed as e:
#     print("Database Already exists")
#     db = server['cloud-couchdb']

image_file = open("testing_dataset/650KB.jpg", 'rb')
img = base64.b64encode(image_file.read()).hex()
print(type(img))
# exit(0)

payload_for_couchdb = {"input_array": [img, img, img, img, img], "request_id": "22", "data_op_type": "init_put",
                       "application": constants.IMAGE_INFERENCING, "fusion_depth_index": 1, "sending_function": "client"}
# key_doc_id_dict = {}
# doc_id, doc_rev = db.save(payload_for_couchdb)
# key_doc_id_dict[1] = doc_id

# get_response = db[doc_id]
#get_response = json.loads(get_response)

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
#
func_socket.close()

# print(get_response["1"])
