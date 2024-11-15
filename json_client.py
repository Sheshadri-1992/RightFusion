import zmq
import json
from PIL import Image
import base64
from io import BytesIO


iaas_context = zmq.Context()
iaas_socket = iaas_context.socket(zmq.PUSH)
iaas_socket.connect("tcp://" + "192.168.0.110" + ":10000")


image_file = open("56KB.jpg", 'rb')

# img = image_file.read()
# kb = (len(image_file.read())/1024)
# print("The image size before encoding ", kb)
img = base64.b64encode(image_file.read()).hex()

image_file = open("56KB.jpg", 'rb')
kb = (len(image_file.read())/1024)
print("The image size before encoding ", kb)

input_size = len(img)/1024
total_image_length = input_size * 1
print(type(img), len(img))
print("The image size after encoding ", input_size)
# img = Image.open(BytesIO(img))
img = Image.open("shawshank.jpg")
width, height = img.size
print(img.info["ppi"])
print(width, height)
exit(0)
img.save("file.jpg", 'JPEG', quality=95)
# print(type(img))

arg_request_dict = {"event_payload": "func_exec"}

fusion_group_dict = {1: [['nsfw', 'image_annotation']], 3: [['multi_image_resizing'], ['image_concatenation']]}
fusion_depth_list = list(fusion_group_dict.keys())
fusion_depth_list.sort()

least_resource_dict = {'nsfw': {'xeon': 1}, 'image_annotation': {'xeon': 1}, 'multi_image_resizing': {'xeon': 1},
                       'image_concatenation': {'xeon': 1}}

arg_request_dict = {"input_array": [img], "input_size": input_size, "total_image_length": total_image_length,
                    "request_id": 2, "app": "image_inferencing", "request_type": "execute_function",
                    "depth_to_fusion_group_dict": fusion_group_dict, "resource_dict": least_resource_dict,
                    "fusion_depth_list": fusion_depth_list, "fusion_depth_index": 0,
                    "fusion_group": fusion_group_dict[1][0]}

arg_request_dict = json.dumps(arg_request_dict)
iaas_socket.send_string(arg_request_dict)
# result = iaas_socket.recv_string()
# print(result)
iaas_socket.close()
