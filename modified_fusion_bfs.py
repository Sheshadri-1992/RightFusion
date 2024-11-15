import json
import sys
import apply_qos_rules
import time
import image_inferencing_predictor
import web_inferencing_predictor
import final_fusion_possibilities
import apply_rules
import zmq

if len(sys.argv) != 3:
    print("Usage : python modified_fusion_bfs.py <graph_file.json> <starting_node>")
    exit(0)

application = sys.argv[1]

'''
Reading the application workflow graph
'''
with open(application + ".json") as user_file:
    file_contents = user_file.read()

graph = json.loads(file_contents)
graph = graph["application"]

if sys.argv[2] not in graph:
    print(
        "[Error] Starting node not present in the application workflow, please check the json {0}".format(sys.argv[1]))
    exit(0)

'''
Reading User-Constraints Graph
'''
with open(application + "_user_constraints.json") as user_file:
    file_contents = user_file.read()

user_constraints_graph = json.loads(file_contents)
user_constraints_graph = user_constraints_graph[application]

'''
Vertex Data Structures
'''
visited = set()
in_degree_vertex_dict = {}
out_degree_vertex_dict = {}
parent_vertex_dict = {}
vertex_depth_dict = {}
fusion_group_depth_dict = {}
fusion_group_name_dict = {}
vertex_info_dict = {}
fusion_group_info_dict = {}

'''
Get the prediction model object
'''
image_inferencing_obj = image_inferencing_predictor.ImageInferencing()
image_inferencing_obj.train_all_models()

web_inferencing_obj = web_inferencing_predictor.TextInference()
web_inferencing_obj.train_all_models()

start_time = time.time()

'''
Creating info for each function in the application
It contains Parent function, Child function, User constraints, default max resource
'''


def prepare_info_for_each_vertex(vertex_name: str):
    resource = 3
    user_constraint = None
    if vertex_name in user_constraints_graph:
        user_constraint = user_constraints_graph[vertex_name]

    parent = None
    if (vertex_name in parent_vertex_dict) and (len(parent_vertex_dict[vertex_name]) != 0):
        parent = parent_vertex_dict[vertex_name]

    child = None
    if (vertex_name in graph) and (len(graph[vertex_name]) != 0):
        child = graph[vertex_name]

    info_dict = {"resource": resource, "user_constraint": user_constraint, "parent": parent, "child": child,
                 "vertex_name": vertex_name}
    return info_dict


'''
Populate vertex info for each vertex
'''
for function_name in graph.keys():
    vertex_info_dict[function_name] = prepare_info_for_each_vertex(function_name)

'''
Computing in and out degree for each vertex in the application workflow
'''
for vertex in graph.keys():
    out_degree_vertex_dict[vertex] = 0
    in_degree_vertex_dict[vertex] = 0
    parent_vertex_dict[vertex] = []

for vertex in graph.keys():
    out_degree_vertex_dict[vertex] = len(graph[vertex])
    # print("Vertex {0}, adjacency list {1}".format(vertex, len(graph[vertex])))

    for node in graph[vertex]:
        in_degree_vertex_dict[node] = in_degree_vertex_dict[node] + 1
        parent_vertex_dict[node].append(vertex)


# for vertex in graph.keys():
#     # print("Vertex {0}, In-Degree {1} Out-Degree {2}".format(vertex, in_degree_vertex_dict[vertex], out_degree_vertex_dict[vertex]))
#     print("Vertex {0} : Parent {1}".format(vertex, parent_vertex_dict[vertex]))


def check_valid_fusion_group(fusion_group: list) -> bool:
    if len(fusion_group) == 1:
        return True

    for i in range(0, len(fusion_group) - 1):
        vertex_info = vertex_info_dict[fusion_group[i]]
        # print("Node => ",fusion_group[i], vertex_info["child"])
        if fusion_group[i + 1] not in vertex_info["child"]:
            return False
    return True


'''
Creating fusion group head, group tail
This information will help in connecting triggers
'''


def prepare_info_for_each_fusion_group(fusion_group_name: str, fusion_group: list):
    if fusion_group is None or len(fusion_group) == 0:
        return {}

    fusion_head_function = fusion_group[0]
    fusion_tail_function = fusion_group[-1]

    info_dict = {"fusion_group_name": fusion_group_name, "fusion_head": fusion_head_function,
                 "fusion_tail": fusion_tail_function}
    return info_dict


fusion_group_dict = {}
fusion_group_counter = 1

'''
Modified DFS algorithm for fusion
'''


def dfs(arg_visited, app_graph, node, arg_graph_depth):
    global fusion_group_counter

    if node not in arg_visited:
        arg_visited.add(node)
        vertex_depth_dict[node] = arg_graph_depth
        arg_graph_depth = arg_graph_depth + 1
        '''
        1. Vertex has no parent
        '''
        if len(parent_vertex_dict[node]) == 0:
            fusion_group_dict[fusion_group_counter] = []
            fusion_group_dict[fusion_group_counter].append(node)

        if len(parent_vertex_dict[node]) == 1:
            if out_degree_vertex_dict[parent_vertex_dict[node][0]] == 1:
                if fusion_group_counter not in fusion_group_dict:
                    fusion_group_dict[fusion_group_counter] = []

            if out_degree_vertex_dict[parent_vertex_dict[node][0]] > 1:
                fusion_group_counter = fusion_group_counter + 1
                if fusion_group_counter not in fusion_group_dict:
                    fusion_group_dict[fusion_group_counter] = []

            fusion_group_dict[fusion_group_counter].append(node)

        '''
        If current vertex has more than one parent (fan in case) - No fusion                
        '''

        if len(parent_vertex_dict[node]) > 1:
            fusion_group_counter = fusion_group_counter + 1
            if fusion_group_counter not in fusion_group_dict:
                fusion_group_dict[fusion_group_counter] = []
            fusion_group_dict[fusion_group_counter].append(node)

        for neighbour in app_graph[node]:
            dfs(arg_visited, app_graph, neighbour, arg_graph_depth)


# Driver Code
graph_depth = 1
dfs(visited, graph, sys.argv[2], graph_depth)

fusion_group_list = [fusion_group_dict[x] for x in fusion_group_dict.keys()]

'''
5. From here onwards Edge-Cloud conditions and User constraints have to be applied
'''
function_list = graph.keys()
func_to_app_char_list_dict = {}
for function in function_list:
    func_to_app_char_list_dict[function] = [1870.0, 2] # This is for the web inferencing
    # func_to_app_char_list_dict[function] = [50.0]  # This is for the text inferencing
func_to_qos_and_user_constraint_dict = {}

'''
Read the constraints that are specified by the User
'''
with open(application + "_user_constraints.json") as user_file:
    file_contents = user_file.read()

    constraint_graph = json.loads(file_contents)
    constraint_graph = constraint_graph[application]

print(constraint_graph)
# exit(0)

for function in function_list:
    func_to_qos_and_user_constraint_dict[function] = {"qos_metric": 1500}
    # func_to_qos_and_user_constraint_dict[function] = {"constraint": "xeon", "qos_metric": 1500}
    # func_to_qos_and_user_constraint_dict[function] = {"qos_metric": 100}

if constraint_graph.keys() is None or len(constraint_graph.keys()) == 0:
    pass
else:
    for function in function_list:
        if function in constraint_graph:
            func_to_qos_and_user_constraint_dict[function]["constraint"] = constraint_graph[function]

print("CONSTRAINT ", func_to_qos_and_user_constraint_dict)

resource_spec_dict = image_inferencing_obj.predict_execution_times_for_all_functions(
    func_to_app_char_list_dict, func_to_qos_and_user_constraint_dict)

# resource_spec_dict = web_inferencing_obj.predict_execution_times_for_all_functions(func_to_app_char_list_dict,
#                                                                                    func_to_qos_and_user_constraint_dict)

print("WHY", resource_spec_dict)
partition_result_dict = apply_rules.apply_fusion_constraints(fusion_group_list, user_constraints_graph,
                                                             resource_spec_dict, vertex_depth_dict)
new_fusion_group_list =  partition_result_dict["fusion_group"]
least_resource_dict = partition_result_dict["resource_spec"]

print("Back", new_fusion_group_list)

# final_fusion_obj = final_fusion_possibilities.PropertyBasedDFS({}, {},
#                                                                parent_vertex_dict, out_degree_vertex_dict)
# new_visited = set()
# graph_depth = 1
# final_fusion_obj.property_based_dfs(visited, graph, sys.argv[2], graph_depth, resource_spec_dict)

# new_fusion_group_list = apply_qos_rules.apply_fusion_constraints(fusion_group_list, application, resource_spec_dict,

final_fusion_group_list = []
for fusion_group in new_fusion_group_list:
    if check_valid_fusion_group(fusion_group) is True:
        print("Fusion group {0} is Valid".format(fusion_group))
        final_fusion_group_list.append(fusion_group)

new_fusion_group_list = final_fusion_group_list
print("New fusion group ", new_fusion_group_list)

for individual_group in new_fusion_group_list:
    group_level = 100
    group_name = ""

    for vertex in individual_group:
        group_name = group_name + vertex + ","
        vertex_level = vertex_depth_dict[vertex]
        if vertex_level < group_level:
            group_level = vertex_level

    fusion_group_name_dict[group_name] = individual_group
    fusion_group_depth_dict[group_name] = group_level


'''
Prepare node information for each vertex such as E/C
'''
end_time = time.time()
total_time = (end_time - start_time) * 1000

print("Total time", total_time)
fusion_group_info_dict = {}
for individual_group_name in fusion_group_name_dict.keys():
    fusion_group_info_dict[individual_group_name] = prepare_info_for_each_fusion_group(individual_group_name,
                                                                                       fusion_group_name_dict[
                                                                                           individual_group_name])
print(fusion_group_info_dict)
print(least_resource_dict)
print(fusion_group_name_dict)
print(fusion_group_depth_dict)

final_fusion_group_depth_dict = {}

for key in fusion_group_depth_dict.keys():
    fusion_depth = fusion_group_depth_dict[key]
    print(fusion_depth)
    if fusion_depth not in final_fusion_group_depth_dict:
        final_fusion_group_depth_dict[fusion_depth] = []
    final_fusion_group_depth_dict[fusion_depth].append(fusion_group_name_dict[key])

print("Final fusion group ",final_fusion_group_depth_dict)