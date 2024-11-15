# my_list = ["xeon", "raspberry", "xeon", "xeon", "raspberry"]
# user_constraint = {1: "raspberry", 4: "xeon"}


def perform_resource_equivalent_partition(starting_layer, my_list, user_constraint, fusion_group):
    print("Resource equivalent partition ", starting_layer, my_list, user_constraint, fusion_group)
    resource_breakage_positions = []
    final_break_positions = []

    if starting_layer == "xeon":
        for i in range(0, len(my_list)):
            print(my_list[i])
            if my_list[i] != starting_layer:
                resource_breakage_positions.append(i)

        # check for upgrade
        for ele in resource_breakage_positions:
            if ele in user_constraint:
                user_specified_resource_type = user_constraint[ele]
                if user_specified_resource_type == "xeon":
                    my_list[ele] = "xeon"
                else: # If the user is forcing to run on raspberry
                    final_break_positions.append(ele)
            else:  # even if it is suggested by the recommender as the cheapest resource, upgrade it to xeon,
                # E->C policy
                my_list[ele] = "xeon"

    break_index = 0
    did_break = False

    if starting_layer == "raspberry":
        for i in range(0, len(my_list)):
            if my_list[i] != starting_layer:
                print("Break ", my_list[i], starting_layer)
                break_index = i
                resource_breakage_positions.append(i)
                final_break_positions.append(i)
                did_break = True
                break

        if did_break is True:
            for i in range(break_index, len(my_list)):
                my_list[i] = "xeon"
                resource_breakage_positions.append(i)
                # final_break_positions.append(i) # This would add for the first change from rasp to xeon

        for i in range(0, len(my_list)):
            if i in user_constraint:
                user_specified_resource_type = user_constraint[i]
                if user_specified_resource_type == "raspberry":  # earlier it was raspberry
                    resource_breakage_positions.append(i)

        print("Resource breakage positions => ", resource_breakage_positions)

        # Check for upgrade
        for ele in resource_breakage_positions:
            if ele in user_constraint:
                user_specified_resource_type = user_constraint[ele]
                if user_specified_resource_type == "xeon":
                    my_list[ele] = "xeon"
                else:
                    final_break_positions.append(ele)

    print("Resource Equivalence Partition {0} my list {1}".format(final_break_positions, my_list))
    index_list = final_break_positions

    function_resource_type_dict = {}
    if starting_layer == "raspberry":
        for i in range(0, len(fusion_group)):
            if i in index_list:
                function_resource_type_dict[fusion_group[i]] = "xeon"
            else:
                function_resource_type_dict[fusion_group[i]] = "raspberry"

    if starting_layer == "xeon":
        for i in range(0, len(fusion_group)):
            if i in index_list:
                function_resource_type_dict[fusion_group[i]] = "raspberry"
            else:
                function_resource_type_dict[fusion_group[i]] = "xeon"

    resource_layer = starting_layer
    for ele in my_list:
        if ele == "xeon":
            resource_layer = "xeon"

    if len(index_list) == 0:
        print("No break positions => ", fusion_group)
        return {"fusion_group": [fusion_group], "resource_layer": resource_layer,
                "function_resource_dict": function_resource_type_dict}

    new_fusion_group_list = []
    if len(index_list) != 0:
        if len(index_list) == 1:
            index = index_list[0]
            if index - 1 >= 0:
                list1 = fusion_group[0:index]
                new_fusion_group_list.append(list1)
            if index + 1 <= len(fusion_group):
                list2 = my_list[index + 1:len(fusion_group)]
                new_fusion_group_list.append(list2)

        if len(index_list) > 1:
            min_index = index_list[0]
            max_index = index_list[-1]
            list1 = fusion_group[:min_index]
            list2 = fusion_group[max_index + 1:]
            new_fusion_group_list.append(list1)
            new_fusion_group_list.append(list2)

            for i in range(1, len(index_list)):
                new_list = my_list[index_list[i - 1] + 1: index_list[i]]
                new_fusion_group_list.append(new_list)

        for index in index_list:
            new_fusion_group_list.append([fusion_group[index]])

    print("After partitioning =>", new_fusion_group_list)

    """
    Remove user constraint list elements from the fusion group
    """
    # for key in user_constraint.keys():
    #     item = [fusion_group[key]]
    #     new_fusion_group_list.remove(item)

    constraint_fusion_list = []
    new_index_list = [index_list[0]]
    if len(index_list) >= 2:
        counter = index_list[0]
        for i in range(1, len(index_list)):
            if index_list[i] == index_list[i - 1] + 1:
                print(index_list[i], index_list[i - 1] + 1)
                new_index_list.append(index_list[i])
                continue
            else:
                print(index_list[i])
                constraint_fusion_list.append(new_index_list)
                new_index_list = [index_list[i]]

    constraint_fusion_list.append(new_index_list)
    print("Final index list", constraint_fusion_list)
    for sub_groups in constraint_fusion_list:
        new_group = []
        for ele in sub_groups:
            new_group.append(fusion_group[ele])
        new_fusion_group_list.append(new_group)

    new_fusion_group_list = [x for x in new_fusion_group_list if x != []]
    new_fusion_group_list = [list(tupl) for tupl in {tuple(item) for item in new_fusion_group_list}]

    print("Hopefully this completes the tough part", new_fusion_group_list)
    return {"fusion_group": new_fusion_group_list, "resource_layer": resource_layer,
            "function_resource_dict": function_resource_type_dict}
