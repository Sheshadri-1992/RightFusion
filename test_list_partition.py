my_list = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
index_list = [1, 3, 6, 9]
# index_list = [4]
# index_list = [1, 2, 3, 5, 7, 8, 9]
# index_list = [1, 3, 5, 7, 9]

constraint_fusion_list = []
new_index_list = [index_list[0]]
if len(index_list) >= 2:
    counter = index_list[0]
    for i in range(1, len(index_list)):
        if index_list[i] == index_list[i-1]+1:
            print(index_list[i], index_list[i-1]+1)
            new_index_list.append(index_list[i])
            continue
        else:
            print(index_list[i])
            constraint_fusion_list.append(new_index_list)
            new_index_list = [index_list[i]]

constraint_fusion_list.append(new_index_list)
print("Final index list", constraint_fusion_list)

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
        list2 = my_list[max_index+1:]
        new_fusion_group_list.append(list1)
        new_fusion_group_list.append(list2)

        for i in range(1, len(index_list)):
            new_list = my_list[index_list[i-1]+1: index_list[i]]
            new_fusion_group_list.append(new_list)

    for index in index_list:
        new_fusion_group_list.append([my_list[index]])

print(new_fusion_group_list)
