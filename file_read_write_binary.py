import logging
import math
import time
import json
import base64
from io import BytesIO

import constants
from PIL import Image


# Pillow
# Python-resize-image

def read_file(file_path):
    image_file = open(file_path, 'rb')
    # print(type(base64.b64encode(image_file.read())))
    my_file = base64.b64encode(image_file.read()).hex()
    # data_length_kb = int(math.ceil(len(my_file)/1024))
    return my_file


def write_file(file_path, my_json):
    # image_file = open(file_path, 'wb')
    # print(my_json)
    byte_array = my_json
    byte_array = bytes.fromhex(byte_array)
    print(type(byte_array))
    byte_array = base64.b64decode(byte_array)
    print(type(byte_array))
    im = Image.open(BytesIO(byte_array))
    im.save('image1.png', 'PNG')

result = read_file("10KB.png")
write_file("", result)
