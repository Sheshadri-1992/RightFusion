import json
import constants
import resource_equivalence_partition


def perform_list_partition(index_list, my_list):
    if len(index_list) == 0:
        return [my_list]

    new_fusion_group_list = []
    if len(index_list) != 0:
        if len(index_list) == 1:
            index = index_list[0]
            if index - 1 >= 0:
                list1 = my_list[0:index]
                new_fusion_group_list.append(list1)
            if index + 1 <= len(my_list):
                list2 = my_list[index + 1:len(my_list)]
                new_fusion_group_list.append(list2)

        if len(index_list) > 1:
            min_index = index_list[0]
            max_index = index_list[-1]
            list1 = my_list[:min_index]
            list2 = my_list[max_index:]
            new_fusion_group_list.append(list1)
            new_fusion_group_list.append(list2)

            for i in range(1, len(index_list)):
                new_list = my_list[index_list[i - 1] + 1: index_list[i]]
                new_fusion_group_list.append(new_list)

        for index in index_list:
            new_fusion_group_list.append([my_list[index]])

    return new_fusion_group_list


def apply_fusion_constraints(fusion_group_list: list, user_constraints_graph: dict, resource_spec_dict: dict,
                             vertex_depth_dict):
    """

    :param vertex_depth_dict:
    :param fusion_group_list:
    :param user_constraints_graph:
    :param resource_spec_dict:
    :return:
    """

    print(fusion_group_list)
    print("User constraint graph => ", user_constraints_graph)

    '''
    Go through each fusion group with resource spec for each function
    '''
    fusion_group_depth_dict = {}
    for individual_group in fusion_group_list:
        group_level = 100
        group_name = ""

        for vertex in individual_group:
            group_name = group_name + vertex + ","
            vertex_level = vertex_depth_dict[vertex]
            if vertex_level < group_level:
                group_level = vertex_level

        if group_level not in fusion_group_depth_dict:
            fusion_group_depth_dict[group_level] = []

        fusion_group_depth_dict[group_level].append(individual_group)

    group_depth_list = list(fusion_group_depth_dict.keys())
    group_depth_list.sort()

    '''
    Partitioning based on resource equivalence
    '''

    least_cost_dict = {}
    for function_name in resource_spec_dict.keys():
        if "raspberry" in resource_spec_dict[function_name] and len(resource_spec_dict[function_name]["raspberry"]) > 0:
            resource_spec = resource_spec_dict[function_name]["raspberry"][0]
            least_cost_dict[function_name] = {"raspberry": resource_spec}
        elif "xeon" in resource_spec_dict[function_name]:
            resource_spec = resource_spec_dict[function_name]["xeon"][0]
            least_cost_dict[function_name] = {"xeon": resource_spec}

    print("[Before] Least Cost Dict => ", least_cost_dict)
    starting_func_name = fusion_group_depth_dict[1][0][0]
    resource_layer = list(least_cost_dict[starting_func_name].keys())
    resource_layer = resource_layer[0]
    # resource_layer = "raspberry"

    '''
    Perform layer equivalent partitioning
    '''

    new_fusion_groups = []
    for group_depth in group_depth_list:
        print("Group Depth {0} Fusion Groups {1}".format(group_depth, fusion_group_depth_dict[group_depth]))

        for individual_group in fusion_group_depth_dict[group_depth]:
            resource_type_list = []
            local_index_fn_dict = {}
            user_constraint_dict = {}
            for i in range(0, len(individual_group)):
                local_index_fn_dict[i] = individual_group[i]
                resource_spec = least_cost_dict[individual_group[i]]
                resource_spec = list(resource_spec.keys())
                resource_spec = resource_spec[0]
                resource_type_list.append(resource_spec)

                if individual_group[i] in user_constraints_graph:
                    user_constraint_dict[i] = individual_group[i]

            print("Resource Layer {0} = Resource Type List {1} = User Constraint Dict {2} = Individual Group {3}"
                  .format(resource_layer, resource_type_list, user_constraint_dict, individual_group))
            result_fusion_groups_dict = resource_equivalence_partition.perform_resource_equivalent_partition(
                resource_layer,
                resource_type_list,
                user_constraint_dict, individual_group)

            print("Result fusion group dict =>", result_fusion_groups_dict)
            resource_layer = result_fusion_groups_dict["resource_layer"]
            new_fusion_groups = new_fusion_groups + (result_fusion_groups_dict["fusion_group"])
            func_resource_dict = result_fusion_groups_dict["function_resource_dict"]
            print("Function resource dict =>", func_resource_dict)

            for individual_fusion_group in result_fusion_groups_dict["fusion_group"]:
                for function in individual_fusion_group:
                    print("Function ", function)
                    if function in least_cost_dict:
                        print("XXSdsf =>", least_cost_dict[function])
                        assigned_resource = func_resource_dict[function]
                        print("Function {0} Assigned Resource {1} ".format(function, assigned_resource))
                        if assigned_resource not in least_cost_dict[function]:
                            if assigned_resource == constants.XEON:
                                least_cost_dict[function] = {resource_layer: 1}
                            if assigned_resource == constants.RASPBERRY:
                                least_cost_dict[function] = {resource_layer: 3}

    print("[After] Least Cost Dict => ", least_cost_dict)

    '''
    Break fusion if resource spec equivalence is not met
    '''
    print("New fusion groups => ", new_fusion_groups)
    next_level_fusion_group = []
    for fusion_group in new_fusion_groups:
        if len(fusion_group) == 1:
            next_level_fusion_group.append(fusion_group)
            continue

        min_resource_for_group = constants.MAX_VCPU
        for function in fusion_group:
            print("FUSION GROUP ", function)
            resource_spec = least_cost_dict[function]
            resource_spec = list(resource_spec.keys())
            resource_spec = resource_spec[0]
            least_resource_spec = least_cost_dict[function][resource_spec]

            if least_resource_spec < min_resource_for_group:
                min_resource_for_group = least_resource_spec

        print(min_resource_for_group)
        if len(fusion_group) > 1:
            index_list_where_resource_is_not_minimum = []
            for i in range(0, len(fusion_group)):
                print(fusion_group[i], least_cost_dict[fusion_group[i]])
                resource_spec = least_cost_dict[fusion_group[i]]
                resource_spec = list(resource_spec.keys())
                resource_spec = resource_spec[0]
                func_resource_spec = least_cost_dict[fusion_group[i]][resource_spec]
                print("Inside here =>", fusion_group[i], func_resource_spec, min_resource_for_group)

                if func_resource_spec > min_resource_for_group:
                    index_list_where_resource_is_not_minimum.append(i)

            print("Index list where resource not minimum", index_list_where_resource_is_not_minimum)
            fusion_list = perform_list_partition(index_list_where_resource_is_not_minimum, fusion_group)
            print("Partitioned fusion list =>", fusion_list)
            for ele in fusion_list:
                if len(ele) != 0:
                    next_level_fusion_group.append(ele)
        else:
            next_level_fusion_group.append(fusion_group)

    items_to_be_removed = []
    for groups in next_level_fusion_group:
        if len(groups) > 1:
            for single_func in groups:
                items_to_be_removed.append([single_func])

    for item in items_to_be_removed:
        if item in next_level_fusion_group:
            next_level_fusion_group.remove(item)

    return {"fusion_group": next_level_fusion_group, "resource_spec": least_cost_dict}
