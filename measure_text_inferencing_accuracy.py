import time
import zmq
import json
import socket
from io import BytesIO
import constants
import logging
import web_inferencing_predictor

logging.basicConfig(level=logging.INFO)

text_file_list = ['3KB.json', '6KB.json', '10KB.json', '12KB.json', '13KB.json', '15KB.json', '25KB.json', '30KB.json',
                  '40KB.json', '50KB.json', '54KB.json', '63KB.json', '73KB.json', '80KB.json', '89KB.json',
                  '97KB.json']

text_index_array = [12, 0, 0, 9, 6, 4, 5, 0, 3, 6, 3, 1, 13, 0, 9, 15, 1, 3, 4, 7, 6, 14, 12, 6, 9, 11, 2, 6, 6, 0, 11,
                    9, 6,
                    1, 0, 0, 10, 1, 2, 7, 6, 5, 2, 10, 11, 8, 0, 0, 3, 12, 0, 1, 8, 6, 3, 7, 8, 4, 8, 8, 2, 3, 8, 7, 10,
                    6,
                    5, 12, 7, 13, 1, 12, 13, 4, 2, 11, 6, 5, 6, 2, 10, 5, 5, 11, 0, 4, 6, 12, 3, 12, 12, 10, 0, 14, 5,
                    13, 0,
                    0, 6, 0]

text_index_array = text_index_array + text_index_array + text_index_array[:50]


if __name__ == "__main__":

    text_pipeline_obj = web_inferencing_predictor.TextInference()
    text_pipeline_obj.train_all_models()
    req_count = 1
    with open("text_inf_new_decision_tree_prediction_times.txt", 'a') as text_file_output:
        for i in range(0, len(text_file_list)):
        #for i in range(0, 3):
            print(req_count)
            index = i
            print(index)
            filename = "text_inferencing_fusion_functions/new_text_testing_dataset/" + text_file_list[index]
            print(filename)

            text_file = open(filename, 'rb')
            kb = (len(text_file.read()) / 1024)
            input_size = kb
            print(kb)
            prediction_array_input = [input_size, 1]
            output = text_pipeline_obj.test_predict(input_size, 1)
            print(output)
            output["input_size"] = int(input_size)
            text_file_output.write(json.dumps(output))
            text_file_output.write("\n")
            time.sleep(0.1)
            req_count = req_count + 1
            # print(output)
