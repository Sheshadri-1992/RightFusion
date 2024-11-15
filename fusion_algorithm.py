import json
import sys
import apply_qos_rules
import time

import constants
import face_detection_predictor
import image_inferencing_predictor
import web_inferencing_predictor
import final_fusion_possibilities
import apply_rules
import zmq
import socket
import logging

logging.basicConfig(level=logging.DEBUG)


class FusionAlgorithm:
    def __init__(self):
        """
        Vertex Data Structures
        """
        self.visited = set()
        self.web_inferencing_obj = None
        self.image_inferencing_obj = None
        self.face_detection_obj = None
        self.in_degree_vertex_dict = {}
        self.out_degree_vertex_dict = {}
        self.parent_vertex_dict = {}
        self.vertex_depth_dict = {}
        self.fusion_group_depth_dict = {}
        self.fusion_group_name_dict = {}
        self.vertex_info_dict = {}
        self.fusion_group_info_dict = {}
        self.fusion_group_dict = {}
        self.fusion_group_list = []
        self.fusion_group_counter = 1
        self.graph = {}
        self.user_constraints_graph = {}

    '''
    Creating info for each function in the application
    It contains Parent function, Child function, User constraints, default max resource
    '''

    def prepare_info_for_each_vertex(self, vertex_name: str):
        resource = 3
        user_constraint = None
        if vertex_name in self.user_constraints_graph:
            user_constraint = self.user_constraints_graph[vertex_name]

        parent = None
        if (vertex_name in self.parent_vertex_dict) and (len(self.parent_vertex_dict[vertex_name]) != 0):
            parent = self.parent_vertex_dict[vertex_name]

        child = None
        if (vertex_name in self.graph) and (len(self.graph[vertex_name]) != 0):
            child = self.graph[vertex_name]

        info_dict = {"resource": resource, "user_constraint": user_constraint, "parent": parent, "child": child,
                     "vertex_name": vertex_name}

        return info_dict

    def check_valid_fusion_group(self, fusion_group: list) -> bool:
        if len(fusion_group) == 1:
            return True

        for i in range(0, len(fusion_group) - 1):
            vertex_info = self.vertex_info_dict[fusion_group[i]]
            # print("Node => ",fusion_group[i], vertex_info["child"])
            if fusion_group[i + 1] not in vertex_info["child"]:
                return False
        return True

    '''
    Creating fusion group head, group tail
    This information will help in connecting triggers
    '''

    def prepare_info_for_each_fusion_group(self, fusion_group_name: str, fusion_group: list):
        if fusion_group is None or len(fusion_group) == 0:
            return {}

        fusion_head_function = fusion_group[0]
        fusion_tail_function = fusion_group[-1]

        info_dict = {"fusion_group_name": fusion_group_name, "fusion_head": fusion_head_function,
                     "fusion_tail": fusion_tail_function}
        return info_dict

    '''
    Modified DFS algorithm for fusion
    '''

    def dfs(self, arg_visited, app_graph, node, arg_graph_depth):
        # global fusion_group_counter

        if node not in arg_visited:
            arg_visited.add(node)
            self.vertex_depth_dict[node] = arg_graph_depth
            arg_graph_depth = arg_graph_depth + 1
            '''
            1. Vertex has no parent
            '''
            if len(self.parent_vertex_dict[node]) == 0:
                self.fusion_group_dict[self.fusion_group_counter] = []
                self.fusion_group_dict[self.fusion_group_counter].append(node)

            if len(self.parent_vertex_dict[node]) == 1:
                if self.out_degree_vertex_dict[self.parent_vertex_dict[node][0]] == 1:
                    if self.fusion_group_counter not in self.fusion_group_dict:
                        self.fusion_group_dict[self.fusion_group_counter] = []

                if self.out_degree_vertex_dict[self.parent_vertex_dict[node][0]] > 1:
                    self.fusion_group_counter = self.fusion_group_counter + 1
                    if self.fusion_group_counter not in self.fusion_group_dict:
                        self.fusion_group_dict[self.fusion_group_counter] = []

                self.fusion_group_dict[self.fusion_group_counter].append(node)

            '''
            If current vertex has more than one parent (fan in case) - No fusion                
            '''

            if len(self.parent_vertex_dict[node]) > 1:
                fusion_group_counter = self.fusion_group_counter + 1
                if self.fusion_group_counter not in self.fusion_group_dict:
                    self.fusion_group_dict[self.fusion_group_counter] = []
                self.fusion_group_dict[fusion_group_counter].append(node)

            for neighbour in app_graph[node]:
                self.dfs(arg_visited, app_graph, neighbour, arg_graph_depth)

    # application = sys.argv[1]

    def read_graph_and_user_constraints(self, application):
        """
        Reading the application workflow graph
        """
        with open(application + ".json") as user_file:
            file_contents = user_file.read()

        self.graph = json.loads(file_contents)
        self.graph = self.graph["application"]

        # if sys.argv[2] not in graph:
        #     print(
        #         "[Error] Starting node not present in the application workflow, please check the json {0}".format(sys.argv[1]))
        #     exit(0)

        '''
        Reading User-Constraints Graph
        '''
        with open(application + "_user_constraints.json") as constraints_file:
            constraints_file_contents = constraints_file.read()

        self.user_constraints_graph = json.loads(constraints_file_contents)
        self.user_constraints_graph = self.user_constraints_graph[application]

    def train_ml_models(self):
        """
        Get the prediction model object
        """
        self.image_inferencing_obj = image_inferencing_predictor.ImageInferencing()
        self.image_inferencing_obj.new_train_all_models()

        self.web_inferencing_obj = web_inferencing_predictor.TextInference()
        self.web_inferencing_obj.train_all_models()

        self.face_detection_obj = face_detection_predictor.FaceDetectionPipeline()
        self.face_detection_obj.new_train_all_models()

    def prepare_vertex_info_for_vertex(self):
        """
        Populate vertex info for each vertex
        """
        for function_name in self.graph.keys():
            self.vertex_info_dict[function_name] = self.prepare_info_for_each_vertex(function_name)

    def compute_in_and_out_degree_for_vertices(self):
        """
        Computing in and out degree for each vertex in the application workflow
        """
        for vertex in self.graph.keys():
            self.out_degree_vertex_dict[vertex] = 0
            self.in_degree_vertex_dict[vertex] = 0
            self.parent_vertex_dict[vertex] = []

        for vertex in self.graph.keys():
            self.out_degree_vertex_dict[vertex] = len(self.graph[vertex])
            # print("Vertex {0}, adjacency list {1}".format(vertex, len(graph[vertex])))

            for node in self.graph[vertex]:
                self.in_degree_vertex_dict[node] = self.in_degree_vertex_dict[node] + 1
                self.parent_vertex_dict[node].append(vertex)

            # for vertex in graph.keys():
            #     # print("Vertex {0}, In-Degree {1} Out-Degree {2}".format(vertex, in_degree_vertex_dict[vertex], out_degree_vertex_dict[vertex]))
            #     print("Vertex {0} : Parent {1}".format(vertex, parent_vertex_dict[vertex]))

    def run_dfs(self, arg_starting_function):
        # Driver Code
        graph_depth = 1
        self.dfs(self.visited, self.graph, arg_starting_function, graph_depth)

        self.fusion_group_list = [self.fusion_group_dict[x] for x in self.fusion_group_dict.keys()]
        print(self.fusion_group_list)

    def apply_fusion_constraints(self, application, arg_input_dimension: list, arg_func_to_qos_dict: dict):
        """
        5. From here onwards Edge-Cloud conditions and User constraints have to be applied
        """
        function_list = self.graph.keys()
        func_to_app_char_list_dict = {}
        for function in function_list:
            func_to_app_char_list_dict[function] = arg_input_dimension  # This is for the web inferencing
            # func_to_app_char_list_dict[function] = [1870.0, 2]  # This is for the web inferencing
            # func_to_app_char_list_dict[function] = [50.0]  # This is for the text inferencing
        func_to_qos_and_user_constraint_dict = {}

        # '''
        # Read the constraints that are specified by the User
        # '''
        # with open(application + "_user_constraints.json") as user_file:
        #     file_contents = user_file.read()
        #
        #     constraint_graph = json.loads(file_contents)
        #     constraint_graph = constraint_graph[application]
        #
        # print(constraint_graph)
        # exit(0)

        for function in function_list:
            if function in arg_func_to_qos_dict:
                func_to_qos_and_user_constraint_dict[function] = arg_func_to_qos_dict[function]
            # func_to_qos_and_user_constraint_dict[function] = {"constraint": "xeon", "qos_metric": 1500}
            # func_to_qos_and_user_constraint_dict[function] = {"qos_metric": 100}

        if self.user_constraints_graph.keys() is None or len(self.user_constraints_graph.keys()) == 0:
            pass
        else:
            for function in function_list:
                if function in self.user_constraints_graph:
                    func_to_qos_and_user_constraint_dict[function]["constraint"] = self.user_constraints_graph[function]

        print("[Fusion Algorithm] Func to QoS user constraint ", func_to_qos_and_user_constraint_dict)

        if application == constants.IMAGE_INFERENCING:
            resource_spec_dict = self.image_inferencing_obj.predict_execution_times_for_all_functions(
                func_to_app_char_list_dict, func_to_qos_and_user_constraint_dict)
        elif application == constants.TEXT_INFERENCING:
            resource_spec_dict = self.web_inferencing_obj.predict_execution_times_for_all_functions(
                func_to_app_char_list_dict, func_to_qos_and_user_constraint_dict)
        elif application == constants.FACE_DETECTION_PIPELINE:
            resource_spec_dict = self.face_detection_obj.predict_execution_times_for_all_functions(
                func_to_app_char_list_dict, func_to_qos_and_user_constraint_dict)

        print("[Fusion Algorithm] Resource Spec Dict {0}".format(resource_spec_dict))

        partition_result_dict = apply_rules.apply_fusion_constraints(self.fusion_group_list,
                                                                     self.user_constraints_graph,
                                                                     resource_spec_dict, self.vertex_depth_dict)
        new_fusion_group_list = partition_result_dict["fusion_group"]
        least_resource_dict = partition_result_dict["resource_spec"]

        final_fusion_group_list = []
        for fusion_group in new_fusion_group_list:
            if self.check_valid_fusion_group(fusion_group) is True:
                print("Fusion group {0} is Valid".format(fusion_group))
                final_fusion_group_list.append(fusion_group)

        new_fusion_group_list = final_fusion_group_list
        print("New fusion group ", new_fusion_group_list)

        for individual_group in new_fusion_group_list:
            group_level = 100
            group_name = ""

            for vertex in individual_group:
                group_name = group_name + vertex + ","
                vertex_level = self.vertex_depth_dict[vertex]
                if vertex_level < group_level:
                    group_level = vertex_level

            self.fusion_group_name_dict[group_name] = individual_group
            self.fusion_group_depth_dict[group_name] = group_level

        fusion_group_info_dict = {}
        for individual_group_name in self.fusion_group_name_dict.keys():
            fusion_group_info_dict[individual_group_name] = (self.prepare_info_for_each_fusion_group
                                                             (individual_group_name,
                                                              self.fusion_group_name_dict[individual_group_name]))
        print(fusion_group_info_dict)
        print(least_resource_dict)
        print(self.fusion_group_name_dict)
        print(self.fusion_group_depth_dict)

        final_fusion_group_depth_dict = {}

        for key in self.fusion_group_depth_dict.keys():
            fusion_depth = self.fusion_group_depth_dict[key]
            print(fusion_depth)
            if fusion_depth not in final_fusion_group_depth_dict:
                final_fusion_group_depth_dict[fusion_depth] = []
            final_fusion_group_depth_dict[fusion_depth].append(self.fusion_group_name_dict[key])

        logging.info("[Fusion Algorithm] Final fusion group {0}".format(final_fusion_group_depth_dict))
        return {"resource_dict": least_resource_dict, "fusion_group_dict": final_fusion_group_depth_dict}

    def clear_graph_data(self):
        """
        Clear all the graph data structures
        """
        # self.web_inferencing_obj = None
        # self.image_inferencing_obj = None
        self.visited = set()
        self.in_degree_vertex_dict = {}
        self.out_degree_vertex_dict = {}
        self.parent_vertex_dict = {}
        self.vertex_depth_dict = {}
        self.fusion_group_depth_dict = {}
        self.fusion_group_name_dict = {}
        self.vertex_info_dict = {}
        self.fusion_group_info_dict = {}
        self.fusion_group_dict = {}
        self.fusion_group_list = []
        self.fusion_group_counter = 1
        self.graph = {}
        self.user_constraints_graph = {}


if __name__ == "__main__":

    input_dimension = [1870.0, 2]
    func_to_qos_dict = {"nsfw": {"qos_metric": 1500}, "multi_image_resizing": {"qos_metric": 1500},
                        "image_annotation": {"qos_metric": 1500}, "image_concatenation": {"qos_metric": 1500}}

    input_dimension = [50.0]
    func_to_qos_dict = {"profanity": {"qos_metric": 100}, "spam": {"qos_metric": 100},
                        "text_summarization": {"qos_metric": 100}}

    # app = constants.TEXT_INFERENCING
    # starting_func = constants.SPAM

    fusion_algorithm_obj = FusionAlgorithm()
    fusion_algorithm_obj.train_ml_models()

    '''
    Logic to get the local IP address
    '''
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.connect(('8.8.8.8', 1))  # connect() for UDP doesn't send packets
    local_ip_address = s.getsockname()[0]

    '''
    ZMQ related code
    '''
    context = zmq.Context()
    my_socket = context.socket(zmq.REP)
    my_socket.bind("tcp://" + local_ip_address + ":13000")

    logging.info("[Fusion Algorithm] Started send requests to tcp://{0}:13000".format(local_ip_address))

    while True:
        request_dict = my_socket.recv_pyobj()
        request_type = request_dict["request_type"]

        result = {}

        if request_type == "get_fusion_group":
            func_to_qos_dict = request_dict["qos_requirement"]
            input_dimension = request_dict["input_dimension"]
            app = request_dict["application"]
            starting_func = request_dict["starting_function"]

            fusion_algorithm_obj.read_graph_and_user_constraints(app)
            fusion_algorithm_obj.compute_in_and_out_degree_for_vertices()
            fusion_algorithm_obj.prepare_vertex_info_for_vertex()
            fusion_algorithm_obj.run_dfs(starting_func)
            result = fusion_algorithm_obj.apply_fusion_constraints(app, input_dimension, func_to_qos_dict)
            fusion_algorithm_obj.clear_graph_data()

            print("Fusion algo ", result)

        my_socket.send_pyobj(result)
