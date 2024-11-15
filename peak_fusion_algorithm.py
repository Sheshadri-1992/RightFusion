import zmq
import socket
import logging

# my_dict = {'depth_to_fusion_group_dict': {1: [['nsfw']], 2: [['image_annotation']],
#                                           3: [['multi_image_resizing'], ['image_concatenation']]},
#            'resource_dict': {'nsfw': {'xeon': 3}, 'image_annotation': {'xeon': 1}, 'multi_image_resizing': {'xeon': 1},
#                              'image_concatenation': {'xeon': 1}},
#            'fusion_depth_list': [1, 2, 3], 'fusion_depth_index': 0, 'fusion_group': ['nsfw'],
#            'layer': 'xeon'}

my_dict = {'resource_dict': {'nsfw': {'xeon': 2}, 'image_annotation': {'xeon': 2}, 'multi_image_resizing': {'xeon': 1},
                   'image_concatenation': {'xeon': 1}},
 'fusion_group_dict': {1: [['nsfw', 'image_annotation']], 3: [['multi_image_resizing'], ['image_concatenation']]}}

# my_dict = {'resource_dict': {'spam': {'xeon': 1}, 'profanity': {'xeon': 1}, 'text_summarization': {'xeon': 1}},
#            'fusion_group_dict': {1: [['spam', 'profanity', 'text_summarization']]}}

logging.basicConfig(level=logging.DEBUG)

if __name__ == '__main__':
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
        logging.info("Request received ")
        my_socket.send_pyobj(my_dict)
