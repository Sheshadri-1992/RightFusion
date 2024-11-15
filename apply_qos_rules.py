import json

def apply_fusion_constraints(fusion_group_list: list, application_name: str, resource_spec_dict: dict, vertex_info_dict):
    with open(application_name + "_user_constraints.json") as user_file:
        file_contents = user_file.read()

    constraint_graph = json.loads(file_contents)
    constraint_graph = constraint_graph[application_name]

    if constraint_graph.keys() is None or len(constraint_graph.keys()) == 0:
        return fusion_group_list

    '''
    After applying user constraints we should get a new fusion group 
    (which can be same or different from the original fusion group)
    '''
    new_fusion_group_list = []

    for i in range(len(fusion_group_list)):
        individual_group = fusion_group_list[i]

        for function_name in constraint_graph.keys():

            if function_name in individual_group:
                index = individual_group.index(function_name)

                new_fusion_group_list.append([individual_group[index]])
                if index - 1 >= 0:
                    list1 = individual_group[0:index]
                    # print("List 1", list1)
                    new_fusion_group_list.append(list1)
                if index + 1 <= len(individual_group):
                    list2 = individual_group[index + 1:len(individual_group)]
                    # print("List 2", list2)
                    new_fusion_group_list.append(list2)
            else:
                new_fusion_group_list.append(individual_group)

    # print("Whats here", new_fusion_group_list)

    index_list = []
    for function_name in constraint_graph.keys():
        for i in range(len(new_fusion_group_list)):
            if function_name in new_fusion_group_list[i]:
                new_fusion_group_list[i].remove(function_name)


    for function_name in constraint_graph.keys():
        new_fusion_group_list.append([function_name])

    new_fusion_group_list = [x for x in new_fusion_group_list if x != []]
    # print("apply qos rules", new_fusion_group_list)
    new_fusion_group_list = [list(tupl) for tupl in {tuple(item) for item in new_fusion_group_list}]
    return new_fusion_group_list
