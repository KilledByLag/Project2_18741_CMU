import socket as st
import threading as thr
import os
import sys
import json
import time
import re


#Simple Node Structure that stores all details of a particular Node
class Node:

    # init
    def __init__(self, filename):
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
        self.host = "localhost"

        # Structures for storing neighbor details
        self.neighbors = {}  
        self.active_neighbors = {}
        self.active_neighbors_last_seen = {}
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
        self.NODE_DEATH_TIMEOUT = 5 #Death Declaration threshold
        self.CLEANUP_PERIODICITY = 0.5 #Cleanup thread check periddicity
        self.SERVER_WAIT_TIME = 2 #Server Socket Recieve timeout - Raises exception which is ignored, and tries again
        self.CLIENT_MESSAGE_PERIODICITY = 1 #Client message periodicity
            

    # Show details of node
    def show_details(self, param):
        print(getattr(self, param))

    def get_message(self, metric):
        with self.lock:
            message = {
                "name": self.name,
                "uuid": self.uuid,
                "host": self.host,
                "port": self.port,
                "mydistancetoyou": metric,
                "neighbors": self.active_neighbors
            }
        return message
    
    def update_details(self, message_details):

        def update_active_neighbors(message, addr):
            with self.lock:
                neighbor_node_name, neighbor_node_uuid, metric_to_me = message["name"], message["uuid"], message["mydistancetoyou"]
                # neighbor_node_neighbors = message["neighbors"]

                if neighbor_node_name in self.active_neighbors:
                    self.active_neighbors_last_seen[neighbor_node_name] = time.time()

                else:
                    self.active_neighbors[neighbor_node_name] = {"uuid": neighbor_node_uuid,
                                                "host": addr[0],
                                                "backend_port": addr[1],
                                                "metric": metric_to_me
                                                }
                    self.active_neighbors_last_seen[neighbor_node_name] = time.time()
                    
        def update_neighbor_list(message, addr):
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

        def update_map(message, addr):

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

                #Adding map cleanup here to make it simpler

                map_removal_list = []
                for node in self.map:
                    if node not in self.active_neighbors:
                        map_removal_list.append(node)
                for node in map_removal_list:
                    del self.map[node]

                #Adding own neighbors to map after cleanup because this has to exist

                self.map[self.name] = my_neighbors
  
        def update_rank():
            pass



        """
        Input Args : Message, "from" address
        Function : Updates Active Neigbors, Rank, and Map of the node

        """
        message, addr = message_details
        message = json.loads(message.decode('utf-8'))
        # print(message)

        #Condition to check if it is direct message or forwarded message (Only update active neighbors, if direct message)
        if addr == ((message["host"] if message["host"] == "127.0.0.1" else "127.0.0.1", message["port"])):
            #Update Active Neighbors
            update_active_neighbors(message, addr)
            update_neighbor_list(message, addr)

        update_map(message, addr)
            
            


    # Start as a server
    def start_server(self):
        while not self.event.is_set():
            try:
                self.socket.settimeout(self.SERVER_WAIT_TIME)  # Ensure it doesn't block forever
                message, addr = self.socket.recvfrom(1024)
                if message:
                    self.update_details((message, addr))
            except st.timeout:
                continue  # Timeout ensures the loop remains responsive
            except ConnectionResetError:
                pass

    
    def add_neighbors(self, user_input):
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
        while not self.event.is_set():
            
            try:
                # message = json.dumps(self.get_message()).encode('utf-8')
                for neighbor in self.neighbors:
                    
                    host, port, metric_to_neighbor = self.neighbors[neighbor]["host"], self.neighbors[neighbor]["backend_port"], self.neighbors[neighbor]["metric"]
                    
                    message = json.dumps(self.get_message(metric_to_neighbor)).encode('utf-8')

                    self.socket.sendto(message, (host, int(port)))
                
                self.event.wait(self.CLIENT_MESSAGE_PERIODICITY)
            except Exception as e:
                pass
            
    def extract_details(self, command):
        if command == "neighbors":
            with self.lock:
                return {"neighbors": self.active_neighbors}
        
        if command == "uuid":
            return {"uuid":self.uuid}

        if command == "map":
            with self.lock:
                return {"map": self.map}
            

    def commands(self):
        while not self.event.is_set():
            try:
                user_input = input("Enter command (type 'kill' to quit): ")
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
        while not self.event.wait(timeout=self.CLEANUP_PERIODICITY):
            removal_list = []
            with self.lock:
                for neighbor in self.active_neighbors_last_seen:
                    neighbor_last_active = self.active_neighbors_last_seen[neighbor]
                    if time.time() - neighbor_last_active > self.NODE_DEATH_TIMEOUT:
                        removal_list.append(neighbor)
                for neighbor in removal_list:
                    del self.active_neighbors[neighbor]
                    del self.active_neighbors_last_seen[neighbor]



def main():
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