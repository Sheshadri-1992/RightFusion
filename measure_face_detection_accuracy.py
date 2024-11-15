import time
import sys
import zmq
import json
from PIL import Image
import base64
import socket
from io import BytesIO
import logging
parent_dir = ""
sys.path.append(parent_dir)
import face_detection_predictor


logging.basicConfig(level=logging.INFO)

image_array = ['63KB.jpg', '86KB.jpg', '94KB.jpg', '150KB.jpg', '220KB.jpg', '336KB.jpg', '400KB.jpg', '500KB.jpg',
               '650KB.jpg', '800KB.jpg', '950KB.jpg', '1400KB.jpg', '1600KB.jpg', '1750KB.jpg', '1950KB.jpg',
               '2100KB.jpg']

num_images_array = [1]*250

image_index_array = [8, 11, 12, 4, 5, 7, 15, 5, 8, 7, 8, 13, 3, 12, 0, 11, 8, 14, 7, 12, 8, 5, 1, 1, 7, 4, 5, 7, 8, 12,
                     0, 6, 14, 9, 10, 7, 8, 10, 12, 7, 12, 6, 8, 4, 8, 6, 5, 8, 7, 4, 8, 11, 7, 11, 4, 10, 6, 10, 15, 2,
                     7, 0, 5, 7, 7, 9, 8, 2, 4, 6, 8, 2, 6, 13, 12, 13, 1, 4, 4, 10, 6, 9, 6, 11, 7, 9, 8, 7, 11, 7, 3,
                     6, 12, 9, 6, 9, 10, 7, 13, 11]

image_index_array = image_index_array + image_index_array + image_index_array[:50]
num_images_array = num_images_array + num_images_array + num_images_array[:50]
print(len(image_index_array))
print(len(num_images_array))


# exit(0)
if __name__ == "__main__":

    fd_pipeline_obj = face_detection_predictor.FaceDetectionPipeline()
    fd_pipeline_obj.new_train_all_models()
    req_count = 1
    new_arr = [55.0, 640, 427, 1, 55.0, 2]
    with open("face_detection_new_dataset_knn_regressor1.txt",'a') as fd_file:
        # for i in range(0, len(image_index_array)):
        for i in range(0, len(image_array)):
        # for i in range(0, 1):
            index = image_index_array[i]
            image_path = "new_testing_dataset/" + image_array[i]
            img = Image.open(image_path)
            width = img.size[0]
            height = img.size[1]

            image_file = open(image_path, 'rb')
            kb = (len(image_file.read()) / 1024)
            input_size = kb
            prediction_array_input = [input_size, width, height, 1, input_size, 1]
            output = fd_pipeline_obj.test_predict(prediction_array_input)
            print(output)
            new_dict = {"face_detection" : output["face_detection"]['xeon'][0][0],
                        "format_conversion" : output["format_conversion"]['xeon'][0][0],
                        "image_resizing" : output["image_resizing"]['xeon'][0][0], "image_size": int(input_size)}
            print(int(input_size),",", output["face_detection"]['xeon'][0][0])
            fd_file.write(json.dumps(new_dict))
            fd_file.write("\n")
