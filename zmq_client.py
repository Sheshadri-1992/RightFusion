import zmq
import json
from PIL import Image
import base64
from io import BytesIO


iaas_context = zmq.Context()
iaas_socket = iaas_context.socket(zmq.REQ)
iaas_socket.connect("tcp://" + "192.168.0.110" + ":15000")


image_file = open("530KB.jpg", 'rb')
img = image_file.read()
input_size = len(img)/1024
total_image_length = input_size * 1
print(type(img), len(img))
img = Image.open(BytesIO(img))
# img = Image.open("shawshank.jpg")
# img.save("file.jpg", 'JPEG', quality=95)
print(type(img))

arg_request_dict = {"event_payload": "func_exec"}

arg_request_dict = {"input_array": [img], "input_size": input_size, "total_image_length": total_image_length,
                    "request_id": 2, "app": "image_inferncing"}

iaas_socket.send_pyobj(arg_request_dict)
result = iaas_socket.recv_pyobj()
print(result)
iaas_socket.close()
