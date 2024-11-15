my_list = ["xeon", "raspberry", "xeon", "xeon", "raspberry"]
# user_constraint = {1: "xeon", 4: "xeon"}
user_constraint = {}

resource_breakage_positions = []

# This should come from previous level
starting_layer = "xeon"
# starting_layer = "raspberry"

if starting_layer == "xeon":
    for i in range(0, len(my_list)):
        if my_list[i] != starting_layer:
            resource_breakage_positions.append(i)

    # check for upgrade
    final_break_positions =  []
    for ele in resource_breakage_positions:
        if ele in user_constraint:
            user_specified_resource_type = user_constraint[ele]
            if user_specified_resource_type == "xeon":
                my_list[ele] = "xeon"
            else:
                final_break_positions.append(ele)

break_index = 0
if starting_layer == "raspberry":
    for i in range(0, len(my_list)):
        if my_list[i] != starting_layer:
            break_index = i

    for i in range(break_index, len(my_list)):
        my_list[i] = "xeon"

    resource_breakage_positions = []
    for i in range(0, len(my_list)):
        if i in user_constraint:
            user_specified_resource_type = user_constraint[i]
            if user_specified_resource_type == "raspberry":
                resource_breakage_positions.append(i)

    print("Here",resource_breakage_positions)
    # Check for upgrade
    final_break_positions = []
    for ele in resource_breakage_positions:
        if ele in user_constraint:
            user_specified_resource_type = user_constraint[ele]
            if user_specified_resource_type == "xeon":
                my_list[ele] = "xeon"
            else:
                final_break_positions.append(ele)

print(final_break_positions)