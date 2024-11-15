import json
import sys
import apply_qos_rules
import time

if len(sys.argv) != 3:
    print("Usage : python fusion_bfs.py <graph_file.json> <starting_node>")
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

start_time = time.time()

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

for vertex in graph.keys():
    # print("Vertex {0}, In-Degree {1} Out-Degree {2}".format(vertex, in_degree_vertex_dict[vertex], out_degree_vertex_dict[vertex]))
    print("Vertex {0} : Parent {1}".format(vertex, parent_vertex_dict[vertex]))

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


def dfs(visited, graph, node, graph_depth):
    global fusion_group_counter

    if node not in visited:
        # print (node)
        visited.add(node)
        vertex_depth_dict[node] = graph_depth
        graph_depth = graph_depth + 1
        '''
        1. Vertex has no parent
        '''
        if len(parent_vertex_dict[node]) == 0:
            fusion_group_dict[fusion_group_counter] = []
            fusion_group_dict[fusion_group_counter].append(node)

        '''
        Vertex has parent, but here too, two cases can happen

        2. if current vertex has more than one parent (fan in case) - No fusion                
        '''

        if len(parent_vertex_dict[node]) > 1:
            fusion_group_counter = fusion_group_counter + 1
            if fusion_group_counter not in fusion_group_dict:
                fusion_group_dict[fusion_group_counter] = []
            fusion_group_dict[fusion_group_counter].append(node)

        if (in_degree_vertex_dict[node] == 1) and (
                out_degree_vertex_dict[node] == 1 or out_degree_vertex_dict[node] == 0):

            '''
            3. If parent vertex has an out degree of more than one, the current node needs to have a separate fusion group         
            '''
            for parent in parent_vertex_dict[node]:
                if out_degree_vertex_dict[parent] > 1:
                    fusion_group_counter = fusion_group_counter + 1

            '''
            4. Else the vertex has in-degree 1 out-degree 1, becomes a candidate for fusion

            '''

            if fusion_group_counter not in fusion_group_dict:
                fusion_group_dict[fusion_group_counter] = []

            fusion_group_dict[fusion_group_counter].append(node)

        for neighbour in graph[node]:
            dfs(visited, graph, neighbour, graph_depth)


# Driver Code
graph_depth = 1
dfs(visited, graph, sys.argv[2], graph_depth)

fusion_group_list = [fusion_group_dict[x] for x in fusion_group_dict.keys()]

'''
5. From here onwards Edge-Cloud conditions and User constraints have to be applied
'''
new_fusion_group_list = apply_qos_rules.apply_fusion_constraints(fusion_group_list, application)
print("New fusion group ", new_fusion_group_list)
print(vertex_depth_dict)

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

print(fusion_group_name_dict)
print(fusion_group_depth_dict)

'''
Prepare node information for each vertex such as E/C
'''
end_time = time.time()
total_time = (end_time - start_time) * 1000

print(total_time)
vertex_info_dict = {}
for function_name in graph.keys():
    vertex_info_dict[function_name] = prepare_info_for_each_vertex(function_name)

# print(vertex_info_dict)

fusion_group_info_dict = {}
for individual_group_name in fusion_group_name_dict.keys():
    fusion_group_info_dict[individual_group_name] = prepare_info_for_each_fusion_group(individual_group_name,
                                                                                       fusion_group_name_dict[
                                                                                           individual_group_name])

print(fusion_group_info_dict.keys())
