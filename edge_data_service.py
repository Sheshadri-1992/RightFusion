import json
import logging
import couchdb
import zmq
import socket
from kafka import KafkaProducer
import time
import base64
import docker
import threading

logging.basicConfig(level=logging.INFO)

client = docker.from_env()
container = client.containers.get("couchdb")
couchdb_ip_add = container.attrs['NetworkSettings']['IPAddress']
global_key = 1


def put_couchdb(arg_req_dict: dict):
    global global_key
    logging.info("[Data Service] Put Data is called")
    start_time = time.time()
    payload_for_couchdb = {arg_req_dict["request_id"]: arg_req_dict["input_array"]}
    for key in arg_req_dict:
        if key == "input_array":
            continue
        print(arg_req_dict[key])

    output_dict = json.dumps(payload_for_couchdb)
    # print(output_dict)
    doc_id, doc_rev = db.save(payload_for_couchdb)
    key_doc_id_dict[arg_req_dict["request_id"]] = doc_id
    global_key = global_key + 1
    end_time = time.time()
    total_time = (end_time - start_time) * 1000
    print("[Data Service] CouchDB put Output Size {0}, Time {1}"
          .format(len(output_dict), total_time))

    return {"Status": "Put Successful"}
    # return {"reqid": reqid, "app": app, "output_size": len(output_json), "put_time": total_time}


def get_data(request_id: str) -> json:
    """

    Returns: JSON including the data/appropriate error
    :param request_id:  The input related to request ID

    """
    logging.info("[Data Service] Get data is called")
    try:
        doc_id = key_doc_id_dict[request_id]
        get_response = db[doc_id]
        get_response = get_response[request_id] # This will return an input array in b64 encoded string
        return {"input_array": get_response}
    except KeyError as e:
        logging.info("[Data Service] Key {0} is not in the Database. \n Exception {1}".format(request_id, e))
        return {"status": "key not found"}

    return get_response


def convert_payload_to_json(arg_req_dict: dict):
    application = arg_req_dict["application"]
    if application == "text_inferencing":
        return arg_req_dict

    logging.info("[Data Service] Fusion Depth {0}".format(arg_req_dict["fusion_depth_index"]))

    image_array = arg_req_dict["input_array"]
    new_image_array = []
    for img in image_array:
        img.save("bin_to_json.jpg", 'JPEG')
        image_file = open("bin_to_json.jpg", 'rb')
        # img = image_file.read()

        img = base64.b64encode(image_file.read()).hex()
        new_image_array.append(img)
    arg_req_dict["input_array"] = new_image_array
    return arg_req_dict


def send_req_to_event_manager(arg_req_dict: dict):
    """

    :param arg_req_dict: Output of a function to be saved in the CouchDB, then send it to the event manager
    :return:
    """
    arg_req_dict = convert_payload_to_json(arg_req_dict)
    mutex.acquire()
    put_couchdb(arg_req_dict)
    mutex.release()

    arg_req_dict["input_array"] = []

    bootstrap_servers = ['localhost:9092']
    topic_name = 'fusion'
    producer = KafkaProducer(bootstrap_servers=bootstrap_servers)
    producer.send(topic_name, json.dumps(arg_req_dict).encode())
    producer.flush()

    event_manager_response = {"result": "Put complete, Request forwarded to Event Manager"}
    return event_manager_response


if __name__ == "__main__":

    '''
    Logic to get the local IP address
    '''
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.connect(('8.8.8.8', 1))  # connect() for UDP doesn't send packets
    local_ip_address = s.getsockname()[0]

    packet_holding_dict = {}
    number_of_packets_dict = {}

    '''
    Logic to bind to zmq socket
    '''
    context = zmq.Context()
    # my_socket = context.socket(zmq.PULL)
    my_socket = context.socket(zmq.REP)
    my_socket.bind("tcp://" + local_ip_address + ":12000")

    mutex = threading.Lock()

    print("[Data Service] Service started send requests to tcp://{0}:12000".format(local_ip_address))

    '''
    Couch DB server connection logic
    '''

    server = couchdb.Server('http://admin:password@' + couchdb_ip_add + ':5984/')
    try:
        db = server.create('cloud-couchdb')
    except couchdb.PreconditionFailed as e:
        print("Database Already exists")
        db = server['cloud-couchdb']

    key_doc_id_dict = {}

    while True:
        request_dict = my_socket.recv_pyobj()
        response = {"result": "Unsuccessful"}

        if ("data_op_type" in request_dict) and (request_dict["data_op_type"] == "put"):
            response = send_req_to_event_manager(request_dict)
        elif ("data_op_type" in request_dict) and (request_dict["data_op_type"] == "get"):
            response = get_data(request_dict["request_id"])
        elif ("data_op_type" in request_dict) and (request_dict["data_op_type"] == "init_put"):
            mutex.acquire()
            put_couchdb(request_dict)
            mutex.release()

        my_socket.send_pyobj(response)
