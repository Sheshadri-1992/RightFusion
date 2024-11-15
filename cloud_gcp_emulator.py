import logging
import os
from google.cloud import storage
from gcp_storage_emulator.server import create_server
import constants
import json
import zmq
import socket
from kafka import KafkaProducer
import time
import base64
import threading


class GCPEmulator:
    db: None = None
    logging.basicConfig(level=logging.INFO)

    def __init__(self):

        # default_bucket parameter creates the bucket automatically
        server = create_server(constants.CLOUD_GCP_HOST, constants.CLOUD_GCP_PORT, in_memory=False,
                               default_bucket=constants.CLOUD_GCP_BUCKET)
        server.start()

        os.environ["STORAGE_EMULATOR_HOST"] = f"http://{constants.CLOUD_GCP_HOST}:{constants.CLOUD_GCP_PORT}"
        client = storage.Client()
        self.bucket = client.bucket(constants.CLOUD_GCP_BUCKET)
        self.global_key = 1

    def put_data(self, arg_req_dict: dict) -> dict:

        logging.info("[Data Service] Put Data is called")
        put_start_time = time.time()
        # payload_for_couchdb = {arg_req_dict["request_id"]: arg_req_dict["input_array"]}
        for key in arg_req_dict:
            if key == "input_array":
                continue
            print(arg_req_dict[key])

        image_array_dict = json.dumps(arg_req_dict["input_array"])
        data_blob = self.bucket.blob(arg_req_dict["request_id"])
        data_blob.upload_from_string(image_array_dict)
        print("Uploaded ",len(image_array_dict))

        put_end_time = time.time()
        put_total_time = (put_end_time - put_start_time) * 1000
        print("[Data Service] GCP Emulator put for reg id {0} Output Size {1}, Time {2}".
              format(arg_req_dict["request_id"], len(image_array_dict), put_total_time))

        return {"status": "data inserted"}

    def get_data(self, arg_request_id: str) -> json:
        """

        Args:
            key: Unique identifier of the data to be retrieved

        Returns: JSON including the data/appropriate error

        """
        logging.info("[Data Service] Get Data is called {0}".format(arg_request_id))
        try:
            data_blob = self.bucket.blob(arg_request_id)
            result = data_blob.download_as_string()
            result = json.loads(result)

            logging.info("[GCP_emulator] Downloaded data {0}".format(len(result)))
        except Exception as e:
            logging.info("[GCP_emulator] Key {0} is not in the Database".format(arg_request_id))
            return {"status": "key not found"}

        #return {"input_array": result.decode('utf-8')}
        return {"input_array": result}

    def convert_payload_to_json(self, arg_req_dict: dict):
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

    def send_req_to_event_manager(self, arg_req_dict: dict):
        """

        :param arg_req_dict: Output of a function to be saved in the CouchDB, then send it to the event manager
        :return:
        """
        # arg_req_dict = self.convert_payload_to_json(arg_req_dict)
        mutex.acquire()
        self.put_data(arg_req_dict)
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
    GCP storage emulator logic
    '''
    gcp_storage_obj = GCPEmulator()
    key_doc_id_dict = {}

    data_service_record_file = open("text_inferencing_peak_cloud_gcp_record_file.txt", "a")

    while True:
        request_dict = my_socket.recv_pyobj()
        response = {"result": "Unsuccessful"}

        if ("data_op_type" in request_dict) and (request_dict["data_op_type"] == "put"):
            start_time = time.time()
            print("put sending function ",request_dict["sending_function"])
            response = gcp_storage_obj.send_req_to_event_manager(request_dict)
            response["result"] = "put successful"
            end_time = time.time()
            total_time = (end_time - start_time) * 1000
            record_dict = {"start_time": start_time, "end_time": end_time, "total_time": total_time,
                           "request_id": request_dict["request_id"], "data_op_type": "put",
                           "sending_function": request_dict["sending_function"], "layer": "xeon"}
            logging.info("[Cloud Data Service] put {0}".format(record_dict))
            data_service_record_file.write(json.dumps(record_dict))
            data_service_record_file.write("\n")
            data_service_record_file.flush()

        elif ("data_op_type" in request_dict) and (request_dict["data_op_type"] == "get"):
            start_time = time.time()
            response = gcp_storage_obj.get_data(request_dict["request_id"])
            print("get sending function ",request_dict["sending_function"])
            response["result"] = "get successful"
            end_time = time.time()
            total_time = (end_time - start_time) * 1000
            record_dict = {"start_time": start_time, "end_time": end_time, "total_time": total_time,
                           "request_id": request_dict["request_id"], "data_op_type": "get",
                           "sending_function": request_dict["sending_function"], "layer": "xeon"}
            data_service_record_file.write(json.dumps(record_dict))
            data_service_record_file.write("\n")
            data_service_record_file.flush()

        elif ("data_op_type" in request_dict) and (request_dict["data_op_type"] == "init_put"):
            response["result"] = "init put successful"
            start_time = time.time()
            mutex.acquire()
            gcp_storage_obj.put_data(request_dict)
            mutex.release()
            end_time = time.time()
            total_time = (end_time - start_time) * 1000
            record_dict = {"start_time": start_time, "end_time": end_time, "total_time": total_time,
                           "request_id": request_dict["request_id"], "data_op_type": "init_put",
                           "sending_function": "init_put_client", "layer": "xeon"}
            data_service_record_file.write(json.dumps(record_dict))
            data_service_record_file.write("\n")
            data_service_record_file.flush()

        my_socket.send_pyobj(response)
