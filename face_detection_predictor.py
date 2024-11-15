import time
import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn import datasets, linear_model
from sklearn.tree import DecisionTreeRegressor
from sklearn.neighbors import KNeighborsRegressor

import constants
import warnings

warnings.filterwarnings("ignore", category=DeprecationWarning)


def train_module(file_name):
    df = pd.read_csv(file_name, header=None,
                     names=['total_input_size', 'exec_latency', 'num_images', 'num_cores'])
    X = df[['total_input_size', 'num_images', 'num_cores']].to_numpy()
    Y = df['exec_latency'].to_numpy()
    (X_train, X_test, Y_train, Y_test) = train_test_split(X, Y)
    regr = linear_model.LinearRegression()
    regr.fit(X_train.reshape(-3, 3), Y_train.reshape(-1, 1))
    return regr


def new_train_module(file_name):
    df = pd.read_csv(file_name, header=None,
                     names=['image_size', 'width', 'height', 'num_images', 'total_input_size', 'exec_latency',
                            'num_cores'])
    X = df[['image_size', 'width', 'height', 'num_images', 'total_input_size', 'num_cores']].to_numpy()
    Y = df['exec_latency'].to_numpy()
    (X_train, X_test, Y_train, Y_test) = train_test_split(X, Y, test_size=0.001)

    # if file_name.find("concatenation")
    # regr = linear_model.LinearRegression()
    regr = KNeighborsRegressor(n_neighbors=5)
    # regr = DecisionTreeRegressor(random_state=0)
    # regr.fit(X_train.reshape(-2, 2), Y_train.reshape(-1, 1))
    regr.fit(X_train.reshape(-6, 6), Y_train.reshape(-1, 1))
    return regr


class FaceDetectionPipeline:

    def __init__(self):
        self.xeon_face_det_regr = None
        self.xeon_image_resize_regr = None
        self.xeon_format_conversion_regr = None

        self.rasp_face_det_regr = None
        self.rasp_image_resize_regr = None
        self.rasp_format_conversion_regr = None

    '''
    Train all 3 models here
    '''

    # face_detection_fusion_functions
    # xeon_fd_results.txt
    # xeon_fc_results.txt
    # rasppi4_fc_results.txt
    # rasppi4_fd_results.txt

    def new_train_all_models(self):
        self.rasp_face_det_regr = new_train_module(
            './face_detection_fusion_functions/rasppi4_fd_results.txt')
        self.xeon_face_det_regr = new_train_module(
            './face_detection_fusion_functions/xeon_fd_results.txt')

        self.rasp_image_resize_regr = new_train_module(
            './new_rasp_image_inference_new_data_trace/rasppi4_multi_image_resizing_results.txt')
        self.xeon_image_resize_regr = new_train_module(
            './new_xeon_image_inferencing_results/xeon_multi_image_resizing_results.txt')

        self.rasp_format_conversion_regr = new_train_module(
            './face_detection_fusion_functions/rasppi4_fc_results.txt')
        self.xeon_format_conversion_regr = new_train_module(
            './face_detection_fusion_functions/xeon_fc_results.txt')


    '''
    Returns the resource specification list for a specific resource type
    '''

    def filter_based_on_qos_metric(self, prediction_obj, arg_resource_type,
                                   arg_input_arguments, arg_qos_metric, arg_user_constraint) -> list:
        print("In filter QoS ", arg_input_arguments, arg_resource_type)
        resource_specification_list = []
        for vCPU in range(constants.MIN_VCPU, constants.MAX_VCPU + 1):
            X_test = [x for x in arg_input_arguments]
            X_test.append(vCPU)

            X_test = np.array([X_test])
            X_test.reshape(-6, 6)
            est_exec_time = prediction_obj.predict(X_test)
            print("Est time {0} QoS metric {1}".format(est_exec_time, arg_qos_metric))
            if est_exec_time < arg_qos_metric:
                resource_specification_list.append(vCPU)

        if len(resource_specification_list) == 0 and arg_user_constraint is True:  # arg_resource_type == constants.XEON:
            resource_specification_list.append(constants.MAX_VCPU)

        resource_specification_list.sort()
        return resource_specification_list

    '''
    Predict the execution time for each function that satisfies least cost
    '''

    def predict_execution_time_for_function(self, arg_function: str, arg_resource_type_list: list
                                            , arg_input_arguments: list, arg_qos_metric: int,
                                            arg_user_constraint: bool) -> list:

        resource_type_spec_dict = {}
        if arg_function == constants.FACE_DETECTION:
            print("Here in Face Detection", arg_resource_type_list)
            for arg_resource_type in arg_resource_type_list:
                if arg_resource_type == constants.XEON:
                    resource_specification_list = self.filter_based_on_qos_metric(self.xeon_face_det_regr,
                                                                                  arg_resource_type,
                                                                                  arg_input_arguments,
                                                                                  arg_qos_metric, arg_user_constraint)
                    resource_type_spec_dict[arg_resource_type] = resource_specification_list
                    print(resource_type_spec_dict)
                if arg_resource_type == constants.RASPBERRY:
                    resource_specification_list = self.filter_based_on_qos_metric(self.rasp_face_det_regr,
                                                                                  arg_resource_type,
                                                                                  arg_input_arguments,
                                                                                  arg_qos_metric, arg_user_constraint)
                    resource_type_spec_dict[arg_resource_type] = resource_specification_list

        if arg_function == constants.IMAGE_RESIZING:
            print("Here in Image Resizing", arg_resource_type_list)
            for arg_resource_type in arg_resource_type_list:
                if arg_resource_type == constants.XEON:
                    resource_specification_list = self.filter_based_on_qos_metric(self.xeon_image_resize_regr,
                                                                                  arg_resource_type,
                                                                                  arg_input_arguments,
                                                                                  arg_qos_metric, arg_user_constraint)
                    resource_type_spec_dict[arg_resource_type] = resource_specification_list
                if arg_resource_type == constants.RASPBERRY:
                    resource_specification_list = self.filter_based_on_qos_metric(self.rasp_image_resize_regr,
                                                                                  arg_resource_type,
                                                                                  arg_input_arguments,
                                                                                  arg_qos_metric, arg_user_constraint)
                    resource_type_spec_dict[arg_resource_type] = resource_specification_list

        if arg_function == constants.FORMAT_CONVERSION:
            print("Here in Format Conversion", arg_resource_type_list)
            for arg_resource_type in arg_resource_type_list:
                if arg_resource_type == constants.XEON:
                    resource_specification_list = self.filter_based_on_qos_metric(self.xeon_format_conversion_regr,
                                                                                  arg_resource_type,
                                                                                  arg_input_arguments,
                                                                                  arg_qos_metric, arg_user_constraint)
                    resource_type_spec_dict[arg_resource_type] = resource_specification_list
                if arg_resource_type == constants.RASPBERRY:
                    resource_specification_list = self.filter_based_on_qos_metric(self.rasp_format_conversion_regr,
                                                                                  arg_resource_type,
                                                                                  arg_input_arguments,
                                                                                  arg_qos_metric, arg_user_constraint)
                    resource_type_spec_dict[arg_resource_type] = resource_specification_list

        return resource_type_spec_dict

    def predict_execution_times_for_all_functions(self, func_to_app_char_dict: dict,
                                                  func_to_qos_and_user_constraint_dict: dict):

        resource_spec_dict = {}
        function_list = func_to_app_char_dict.keys()
        for function_name in function_list:
            qos_metric = func_to_qos_and_user_constraint_dict[function_name]["qos_metric"]
            resource_type_list = [constants.XEON, constants.RASPBERRY]
            '''
            If a user constraint is present, predict execution time only on that
            '''
            if "constraint" in func_to_qos_and_user_constraint_dict[function_name]:
                user_constraint = True
                resource_type = func_to_qos_and_user_constraint_dict[function_name]["constraint"]
                app_characteristics_list = func_to_app_char_dict[function_name]
                resource_spec_dict[function_name] = self.predict_execution_time_for_function(function_name,
                                                                                             [resource_type],
                                                                                             app_characteristics_list,
                                                                                             qos_metric,
                                                                                             user_constraint)
            else:
                user_constraint = False
                app_characteristics_list = func_to_app_char_dict[function_name]
                resource_spec_dict[function_name] = self.predict_execution_time_for_function(function_name,
                                                                                             resource_type_list,
                                                                                             app_characteristics_list,
                                                                                             qos_metric,
                                                                                             user_constraint)

        return resource_spec_dict

    def test_predict(self, arg_new_arr):

        # arg_image_size, arg_width, arg_height, arg_num_images, arg_total_img_size, arg_num_cores
        result_json = {}
        X_test = np.array(arg_new_arr)

        face_detection_dict = {}
        face_detection_dict["rasp"] = self.rasp_face_det_regr.predict(X_test.reshape(-6, 6))
        face_detection_dict["xeon"] = self.xeon_face_det_regr.predict(X_test.reshape(-6, 6))

        ir_dict = {}
        ir_dict["rasp"] = self.rasp_image_resize_regr.predict(X_test.reshape(-6, 6))
        ir_dict["xeon"] = self.xeon_image_resize_regr.predict(X_test.reshape(-6, 6))

        format_conversion_dict = {}
        format_conversion_dict["rasp"] = self.rasp_format_conversion_regr.predict(X_test.reshape(-6, 6))
        format_conversion_dict["xeon"] = self.xeon_format_conversion_regr.predict(X_test.reshape(-6, 6))


        result_json["format_conversion"] = format_conversion_dict
        result_json["image_resizing"] = ir_dict
        result_json["face_detection"] = face_detection_dict
        return result_json


# fw_obj = Firewall()
# fw_obj.train_module()
# fw_obj.predict(300, 300)
fd_pipeline_obj = FaceDetectionPipeline()
fd_pipeline_obj.new_train_all_models()
# start_time = time.time()
# # prediction_result = img_inferencing_obj.test_predict(1800.0, 2, 3)
new_arr = [55.0,640,427,1,55.0,1]
# new_arr = [1617.0,6000,4000,5,8087.21,1]
# print(new_arr)
prediction_result = fd_pipeline_obj.test_predict(new_arr)
#
# end_time = time.time()
# total_time = (end_time - start_time) * 1000
# print("Prediction time",total_time)
print(prediction_result)
