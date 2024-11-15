import zmq
import json
from PIL import Image
import base64
import socket
from io import BytesIO
from numpy import random

test_data_images = [
    "2100KB.jpg", "1950KB.jpg", "1750KB.jpg", "1600KB.jpg", "1400KB.jpg", "950KB.jpg", "800KB.jpg", "650KB.jpg",
    "500KB.jpg", "400KB.jpg", "336KB.jpg", "220KB.jpg", "150KB.jpg", "94KB.jpg", "86KB.jpg", "63KB.jpg"]

test_data_images.reverse()

print(test_data_images)
print(len(test_data_images))

x = random.normal(loc=8, scale=4, size=(1, 100))
x = list(x[0])
x = [int(e) for e in x]

num_images_array = []

for i in range(0, len(x)):
    if x[i] < 0:
        x[i] = 0
    if x[i] > 15:
        x[i] = 0

    num_images_array.append(random.randint(1,6))

print(num_images_array)

print(x)
