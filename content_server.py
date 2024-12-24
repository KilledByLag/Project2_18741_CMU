import socket as st
import threading as thr
import sys
import json
import time
import re
import numpy as np

"""
18741 - Computer Networks
Project - 2 Vodserver
Author - Premsai Peddi, CMU Alum (2024)

Description : Content in the Internet is not exclusive to one computer. For instance, an episode of your favorite sitcom on
Netflix will not be stored on one computer, but a network of multiple machines. In this project, you will create
an overlay network that connects machines that replicate common content. You will implement a simple linkstate
routing protocol for the network. We could then use the obtained distance metric to improve our
transport efficiency.

Requirements :
    Libraries - socket, threading, sys, json, time, re, numpy
    Python Version - Python 3.9 or above
    Config Files - Node Details (Provided in the repo)

Working:
    Starts running from CLI
    Usage: python3 /home/student/project2/content_server.py -c /home/student/project2/node21.conf
    Takes in commands continuously - uuid, map, rank, neighbors, addneighbor {respective details}

Outputs:
    UUID - Node identifier
    MAP - Network Map
    RANK - Shortest Path to all neighbors from source
    NEIGHBORS - All Active Neighbors

Note: Brief descriptions for each function and variables are added as comments
WARNING : DO NOT PLAIGARISE!!!! [Will not pass gradescope tests as I've rather sneakily changed some variables]

"""


#Simple Node Structure that stores all details of a particular Node
class Node:

    #Init
    def __init__(self, filename):
        """
        Input: filename (str) - Path to the configuration file
        Output: None
        Function: Initializes the Node with details from the configuration file, sets up neighbors, and binds a socket.

        """
        # Get details from the conf file
        details = {}
        
        with open(fr"{filename}") as f:
            for line in f:
                key, val = line.split("=")
                details[key.strip()] = val.strip()

        # Assign node details
        self.uuid = details['uuid']
        self.name = details['name']
        self.port = int(details['backend_port'])
        self.host = "127.0.0.1"

        # Structures for storing neighbor details
        self.neighbors = {}   #All neighbors available in the config file, and neighbors added during runtime
        self.active_neighbors = {}
        self.neighbors_last_seen = {}
        self.neighbors_seqnums = {}
        self.map = {}
        self.rank = {}

        # Bind socket
        self.socket = st.socket(st.AF_INET, st.SOCK_DGRAM)
        self.socket.bind((self.host, self.port))

        #Adding neighbor details that the node knows by default
        for i in range(int(details['peer_count'])):
            self.neighbors[f"neighbor_{i}"] = {"uuid": details[f'peer_{i}'].split(",")[0].strip(),
                                               "host": details[f'peer_{i}'].split(",")[1].strip(),
                                               "backend_port": int(details[f'peer_{i}'].split(",")[2].strip()),
                                               "metric": int(details[f'peer_{i}'].split(",")[3].strip())}


        #Some Miscellaneous objects
        self.event = thr.Event()
        self.lock =  thr.Lock()
        self.NODE_DEATH_TIMEOUT = 4 #Death Declaration threshold
        self.CLEANUP_PERIODICITY = 0.75 #Cleanup thread check periddicity
        self.SERVER_WAIT_TIME = 0.1 #Server Socket Recieve timeout - Raises exception which is ignored, and tries again
        self.CLIENT_MESSAGE_PERIODICITY = 0.8 #Client message periodicity
            



    def get_message(self, metric, seqnum):
        """
        Input Args : distance to the neighbor, sequence number
        Function: generate message to send to neighbor
        Returns : Dictionary of message

    
        """
        with self.lock:
            message = {
                "name": self.name,
                "uuid": self.uuid,
                "host": self.host,
                "port": self.port,
                "mydistancetoyou": metric,
                "neighbors": self.active_neighbors,
                "seqnum" : seqnum
            }
        return message
    
    def update_details(self, message, addr):
        """

        Input Args : Message, "from" address
        Function : Updates Active Neigbors, Rank, and Map of the node
        Returns : None [Output for commands access the init variables directly]

        """

        def update_active_neighbors(message, addr):
            """
            Input Args: 
                message (dict) - Received message containing node and network details
                addr (tuple) - Address of the sender (host, port)
            Output: None
            Function: Updates the active neighbors list and their last seen timestamps based on the received message.
                      If the neighbor is not already active, adds it to the active neighbors.
            """
            with self.lock:
                neighbor_node_name, neighbor_node_uuid, neighbor_node_host, neighbor_node_port, metric_to_me = message["name"], message["uuid"],message["host"], message["port"], message["mydistancetoyou"]
                # neighbor_node_neighbors = message["neighbors"]

                #Updating last seen time (Improve by simply calling the update_timestamps function later)

                if neighbor_node_name in self.active_neighbors:
                    self.neighbors_last_seen[neighbor_node_name] = time.time()

                else:
                    self.active_neighbors[neighbor_node_name] = {"uuid": neighbor_node_uuid,
                                                "host": neighbor_node_host,
                                                "backend_port": neighbor_node_port,
                                                "metric": metric_to_me
                                                }
                    self.neighbors_last_seen[neighbor_node_name] = time.time()
                    
        def update_neighbor_list(message, addr):
            """
            Input Args: 
                message (dict) - Received message containing node and network details
                addr (tuple) - Address of the sender (host, port)
            Output: None
            Function: Adds a new neighbor to the static neighbors list if the neighbor does not already exist.
            """
            found = False
            with self.lock:
                for neighbor in self.neighbors:
                    if message["uuid"] == self.neighbors[neighbor]["uuid"]:
                        found = True
                        break
                if not found:
                    self.neighbors[f"neighbor_{len(self.neighbors)}"] = {"uuid": message["uuid"],
                                                                         "host": message["host"],
                                                                         "backend_port": message["port"],
                                                                         "metric": message["mydistancetoyou"]}

        def update_timestamps(message, addr):
            """
            Input Args: 
                message (dict) - Received message containing node and network details
                addr (tuple) - Address of the sender (host, port)
            Output: None
            Function: Updates the last seen timestamp of the neighbor in `neighbors_last_seen` if the neighbor is
                      already active. Ensures timestamps are only updated when the current time is later than the
                      stored timestamp.
            """
            with self.lock:
                neighbor_node_name = message["name"]
                current_time = time.time()
                
                # Only update the timestamp if the current time is later
                if neighbor_node_name not in self.neighbors_last_seen or self.neighbors_last_seen[neighbor_node_name] < current_time:
                    self.neighbors_last_seen[neighbor_node_name] = current_time


        def update_map(message, addr):
            """
            Input Args: 
                message (dict) - Received message containing node and network details
                addr (tuple) - Address of the sender (host, port)
            Output: None
            Function: Updates the network map with details of neighbors and their neighbors as received in the
                      message. Ensures the current node's map is updated with its own neighbors and metrics.
            """

            with self.lock:
                my_neighbors = {}
                for neighbor in self.active_neighbors:
                    neighbor, metric = neighbor, self.active_neighbors[neighbor]['metric']
                    my_neighbors[neighbor] = metric
                
                neighbors_neighbors = {}
                for neighbor in message["neighbors"]:
                    neighbor, metric = neighbor, message["neighbors"][neighbor]["metric"]
                    neighbors_neighbors[neighbor] = metric
                
                self.map[message['name']] = neighbors_neighbors

                self.map[self.name] = my_neighbors

  
        def update_rank():
            """
            Input Args: None
            Output: None
            Function: Calculates the shortest paths to all nodes in the network map using Dijkstra's algorithm. Updates
                      the `rank` attribute with sorted distances from the current node to all other nodes in ascending
                      order of distance.
            """
            start_node = self.name

            # Dijkstra's is written for shortest paths
            with self.lock:

                dist = {node: np.inf if node != start_node else 0 for node in self.map}
                prev = {node: None for node in self.map}
                node_set = [node for node in self.map]

                while node_set:
                    u = min((node for node in node_set), key=dist.get)
                    node_set.pop(node_set.index(u))

                    for neighbor in self.map.get(u, {}): 
                        if neighbor in node_set:
                            edge_value = self.map[u][neighbor]
                            alt = dist[u] + edge_value
                            if alt < dist[neighbor]:
                                dist[neighbor] = alt
                                prev[neighbor] = u

                self.rank = {node: value for node, value in dist.items() if node!= self.name}
                self.rank = {k: v for k, v in sorted(self.rank.items(), key=lambda item: item[1])} #Sorted in Ascending order of distance


        #Condition to check if it is direct message or forwarded message (Only update active neighbors, if direct message)
        if addr == ((message["host"], message["port"])):
            update_active_neighbors(message, addr)
            update_neighbor_list(message, addr)
        else:

            update_timestamps(message, addr)

        update_map(message, addr)
        update_rank()
            
            
    # Start as a server
    def start_server(self):
        """
        Input Args: None
        Output: None
        Function: Continuously listens for incoming messages from other nodes, updates the network details,
                  and forwards messages to other neighbors.
        """
        while not self.event.is_set():
            try:
                #This is the receiver part
                self.socket.settimeout(self.SERVER_WAIT_TIME)  # Ensure it doesn't block forever
                message, addr = self.socket.recvfrom(1024)

                if message:
                    message = json.loads(message.decode('utf-8'))

                    if message['name'] not in self.neighbors_seqnums:
                        self.neighbors_seqnums[message['name']] = message['seqnum']

                    
                    if message['seqnum'] > self.neighbors_seqnums[message['name']]:
                        self.neighbors_seqnums[message['name']] = message['seqnum']
                        self.update_details(message, addr)
                        #This part forwards message to all neighbors, except the one it received from
                        try:
                            for neighbor in self.neighbors:
                                host, port = self.neighbors[neighbor]["host"], self.neighbors[neighbor]["backend_port"]

                                if host == "localhost": # Need to handle many of these in the program - ideally declare somewhere that "localhost" == "127.0.0.1" == "0.0.0.0"
                                    host = "127.0.0.1"

                                if ((host, port)) != addr:
                                    self.socket.sendto(json.dumps(message).encode('utf-8'), (host, int(port)))

                        except Exception as e:
                            pass

            except st.timeout:
                continue  # Timeout ensures the loop remains responsive
            except ConnectionResetError:
                pass

    
    def add_neighbors(self, user_input):
        """
        Input Args: 
            user_input (str) - Command to add a new neighbor in the format: 
                               "addneighbor uuid=UUID host=HOST backend_port=PORT metric=METRIC"
        Output: None
        Function: Parses the user command to add a new neighbor to the node.

        """
        # Update the regex pattern to account for quotes around the host
        match_pattern = r'uuid=([\w-]+)\s+host=([\w.-]+)\s+backend_port=(\d+)\s+metric=(\d+)'

        match = re.search(match_pattern, user_input)

        if match:
            new_node = {
                'uuid': match.group(1),
                'host': match.group(2),
                'backend_port': int(match.group(3)),
                'metric': int(match.group(4))
            }
            with self.lock:
                self.neighbors[f"neighbor_{len(self.neighbors)}"] = new_node
            print("Added neighbor Success")
        else:
            print("Addneighbor command format wrong!")


    # Start as client
    def start_client(self):
        """
        Input Args: None
        Output: None
        Function: Periodically sends messages to all known neighbors with the node's details and updates.

        """
        count = 0

        while not self.event.is_set():
            
            try:
                # message = json.dumps(self.get_message()).encode('utf-8')
                for neighbor in self.neighbors:
                    
                    host, port, metric_to_neighbor = self.neighbors[neighbor]["host"], self.neighbors[neighbor]["backend_port"], self.neighbors[neighbor]["metric"]
                    
                    message = json.dumps(self.get_message(metric_to_neighbor, count)).encode('utf-8')

                    self.socket.sendto(message, (host, int(port)))
                
                self.event.wait(self.CLIENT_MESSAGE_PERIODICITY)

            except Exception as e:
                pass
            count += 1
            
    def extract_details(self, command):
        """
        Input Args: 
            command (str) - Command to request specific details ("uuid", "map", "rank", "neighbors")
        Output: dict - The requested details (UUID, network map, rank, or active neighbors)
        Function: Returns specific node or network details based on the given command.

        """
        if command == "neighbors":
            with self.lock:
                return {"neighbors": self.active_neighbors}
        
        if command == "uuid":
            return {"uuid":self.uuid}

        if command == "map":
            with self.lock:
                return {"map": self.map}
            
        if command == "rank":
            with self.lock:
                return {"rank":self.rank}
            

    def commands(self):
        """
        Input Args: None
        Output: None
        Function: Continuously listens for user input commands to perform specific actions like adding neighbors,
                  killing the node, or retrieving details like UUID, map, rank, and neighbors.
        """
        while not self.event.is_set():
            try:
                user_input = input()
                if user_input == "kill":
                    self.event.set()
                    break
                elif user_input.startswith("addneighbor"):
                    self.add_neighbors(user_input)
                else:
                    (print(self.extract_details(user_input)))

            except EOFError:
                self.event.set()
                break
            self.event.wait(1)

    def cleanup_details(self):
        """
        Input Args: None
        Output: None
        Function: Periodically checks for inactive neighbors (based on last seen timestamps) and removes them
                  from the active neighbors, map, and rank.
        """
        while not self.event.wait(timeout=self.CLEANUP_PERIODICITY):
            current_time = time.time()

            with self.lock:
                removal_list = [
                    neighbor for neighbor, last_active in self.neighbors_last_seen.items()
                    if current_time - last_active > self.NODE_DEATH_TIMEOUT
                ]

                for neighbor in removal_list:
                    self.neighbors_last_seen.pop(neighbor, None) #Clean Last seen time
                    self.active_neighbors.pop(neighbor, None) #Clean active neighbors
                    self.map.pop(neighbor, None) #Clean Map
                    self.rank.pop(neighbor, None) #Clean Rank

                    #Clean own neighbors in map, if they are inactive
                    if self.name in self.map: 
                        self.map[self.name].pop(neighbor, None)
                #Clean self, if 0 neighbors active
                if self.name in self.map and not self.map[self.name]:
                    self.map.pop(self.name, None)

def main():
    """
    Input Args: None (reads from sys.argv for configuration file path)
    Output: None
    Function: Initializes the Node, starts server, client, cleanup, and command threads, and manages their lifecycle.

    """

    if len(sys.argv) == 3 and sys.argv[1] == '-c':
        node = Node(sys.argv[2])

        server_thread = thr.Thread(target=node.start_server)
        client_thread = thr.Thread(target=node.start_client)
        cleanup_thread = thr.Thread(target=node.cleanup_details)
        command_thread = thr.Thread(target=node.commands)

        server_thread.start()
        client_thread.start()
        cleanup_thread.start()
        command_thread.start()

        command_thread.join()
        server_thread.join()
        client_thread.join()
        # cleanup_thread.join()
        

if __name__ == "__main__":
    main()
