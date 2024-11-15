class PropertyBasedDFS:

    def __init__(self, vertex_depth_dict, fusion_group_dict, parent_vertex_dict, out_degree_vertex_dict, resource_dict):
        self.vertex_depth_dict = vertex_depth_dict
        self.fusion_group_dict = fusion_group_dict
        self.parent_vertex_dict = parent_vertex_dict
        self.out_degree_vertex_dict = out_degree_vertex_dict
        self.node_constraints_dict = {}
        self.fusion_group_counter = 1
        self.current_node = "edge"
        self.resource_dict = resource_dict


    def property_based_dfs(self, arg_visited, app_graph, node, arg_graph_depth):

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
                self.fusion_group_counter = self.fusion_group_counter + 1
                if self.fusion_group_counter not in self.fusion_group_dict:
                    self.fusion_group_dict[self.fusion_group_counter] = []
                self.fusion_group_dict[self.fusion_group_counter].append(node)

            for neighbour in app_graph[node]:
                self.property_based_dfs(arg_visited, app_graph, neighbour, arg_graph_depth)
