import time
import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn import datasets, linear_model
from sklearn.tree import DecisionTreeRegressor
import constants
import warnings

warnings.filterwarnings("ignore", category=DeprecationWarning)


def train_module(file_name):
    df = pd.read_csv(file_name, header=None,
                     names=['input_size', 'exec_latency', 'num_cores'])
    X = df[['input_size', 'num_cores']].to_numpy()
    Y = df['exec_latency'].to_numpy()
    (X_train, X_test, Y_train, Y_test) = train_test_split(X, Y, test_size=0.001)
    # regr = linear_model.LinearRegression()
    regr = DecisionTreeRegressor(random_state=0)
    regr.fit(X_train.reshape(-2, 2), Y_train.reshape(-1, 1))
    return regr


class TextInference:
    def __init__(self):
        self.xeon_profanity = None
        self.rasp_profanity = None
        self.xeon_text_summarizer = None
        self.rasp_text_summarizer = None
        self.xeon_spam = None
        self.rasp_spam = None

    def train_all_models(self):
        self.xeon_profanity = train_module("./profanity/xeon_profanity_results.txt")
        self.rasp_profanity = train_module("./profanity/rasppi4_profanity_results.txt")

        self.xeon_spam = train_module("./spam/xeon_spam_results.txt")
        self.rasp_spam = train_module("./spam/rasppi4_spam_results.txt")

        self.xeon_text_summarizer = train_module("./text_summarization/xeon_text_summarizer_results.txt")
        self.rasp_text_summarizer = train_module("./text_summarization/rasppi4_text_summarizer_results.txt")

    def filter_based_on_qos_metric(self, prediction_obj, arg_resource_type,
                                   arg_input_arguments, arg_qos_metric, arg_user_constraint) -> list:
        print("In filter QoS ", arg_input_arguments, arg_resource_type)
        resource_specification_list = []
        for vCPU in range(constants.MIN_VCPU, constants.MAX_VCPU + 1):
            X_test = [x for x in arg_input_arguments]
            X_test.append(vCPU)

            print(X_test)

            X_test = np.array([X_test])
            X_test.reshape(-2, 2)
            est_exec_time = prediction_obj.predict(X_test)
            print("Est time {0} QoS metric {1}".format(est_exec_time, arg_qos_metric))
            if est_exec_time < arg_qos_metric:
                resource_specification_list.append(vCPU)

        if len(resource_specification_list) == 0 and arg_user_constraint is True: # arg_resource_type == constants.XEON:
            resource_specification_list.append(constants.MAX_VCPU)

        resource_specification_list.sort()
        return resource_specification_list

    def predict_execution_time_for_function(self, arg_function: str, arg_resource_type_list: list
                                            , arg_input_arguments: list, arg_qos_metric: int, arg_user_constraint: bool) -> list:

        resource_type_spec_dict = {}
        if arg_function == constants.PROFANITY:
            print("Here in PROFANITY", arg_resource_type_list)
            for arg_resource_type in arg_resource_type_list:
                if arg_resource_type == constants.XEON:
                    resource_specification_list = self.filter_based_on_qos_metric(self.xeon_profanity,
                                                                                  arg_resource_type,
                                                                                  arg_input_arguments,
                                                                                  arg_qos_metric, arg_user_constraint)
                    resource_type_spec_dict[arg_resource_type] = resource_specification_list
                    print(resource_type_spec_dict)
                if arg_resource_type == constants.RASPBERRY:
                    resource_specification_list = self.filter_based_on_qos_metric(self.rasp_profanity,
                                                                                  arg_resource_type,
                                                                                  arg_input_arguments,
                                                                                  arg_qos_metric, arg_user_constraint)
                    resource_type_spec_dict[arg_resource_type] = resource_specification_list

        if arg_function == constants.SPAM:
            print("Here in SPAM", arg_resource_type_list)
            for arg_resource_type in arg_resource_type_list:
                if arg_resource_type == constants.XEON:
                    resource_specification_list = self.filter_based_on_qos_metric(self.xeon_spam,
                                                                                  arg_resource_type,
                                                                                  arg_input_arguments,
                                                                                  arg_qos_metric, arg_user_constraint)
                    resource_type_spec_dict[arg_resource_type] = resource_specification_list
                if arg_resource_type == constants.RASPBERRY:
                    resource_specification_list = self.filter_based_on_qos_metric(self.rasp_spam,
                                                                                  arg_resource_type,
                                                                                  arg_input_arguments,
                                                                                  arg_qos_metric, arg_user_constraint)
                    resource_type_spec_dict[arg_resource_type] = resource_specification_list

        if arg_function == constants.TEXT_SUMMARIZATION:
            print("Here in TEXT SUMMARIZATION", arg_resource_type_list)
            for arg_resource_type in arg_resource_type_list:
                if arg_resource_type == constants.XEON:
                    resource_specification_list = self.filter_based_on_qos_metric(self.xeon_text_summarizer,
                                                                                  arg_resource_type,
                                                                                  arg_input_arguments,
                                                                                  arg_qos_metric, arg_user_constraint)
                    resource_type_spec_dict[arg_resource_type] = resource_specification_list
                if arg_resource_type == constants.RASPBERRY:
                    resource_specification_list = self.filter_based_on_qos_metric(self.rasp_text_summarizer,
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
                # print(resource_spec_dict)

        return resource_spec_dict

    def test_predict(self, input_size, num_cores):

        X_test = np.array([input_size, num_cores])
        xeon_result = self.xeon_profanity.predict(X_test.reshape(-2, 2))[0]
        rasp_result = self.rasp_profanity.predict(X_test.reshape(-2, 2))[0]
        profanity_dict = {"xeon": xeon_result, "rasp": rasp_result}
        print(profanity_dict)

        xeon_result = self.xeon_spam.predict(X_test.reshape(-2, 2))[0]
        rasp_result = self.rasp_spam.predict(X_test.reshape(-2, 2))[0]
        spam_dict = {"xeon": xeon_result, "rasp": rasp_result}
        print(spam_dict)

        xeon_result = self.xeon_text_summarizer.predict(X_test.reshape(-2, 2))[0]
        rasp_result = self.rasp_text_summarizer.predict(X_test.reshape(-2, 2))[0]
        text_summarizer_dict = {"xeon": xeon_result, "rasp": rasp_result}
        print(text_summarizer_dict)

        return {"spam": spam_dict["xeon"], "profanity": profanity_dict["xeon"], "text_summarizer": text_summarizer_dict["xeon"]}


# text_inference_obj = TextInference()
# text_inference_obj.train_all_models()
# text_inference_obj.test_predict(50.0, 1)
# print("All models are trained")
