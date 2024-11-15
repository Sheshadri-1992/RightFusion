import zmq
import socket

if __name__ == "__main__":
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
    my_socket.bind("tcp://" + local_ip_address + ":17000")

    print("[Fusion Worker] Service started send requests to tcp://{0}:17000".format(local_ip_address))

    request_dict = my_socket.recv()
    my_socket.send(request_dict)