import socket as st
import threading as thr
import os
import sys
import json
import time
import re
import queue


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
        self.host = "127.0.0.1"

        # Structures for storing neighbor details
        self.neighbors = {}  
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
        self.NODE_DEATH_TIMEOUT = 5 #Death Declaration threshold
        self.CLEANUP_PERIODICITY = 0.5 #Cleanup thread check periddicity
        self.SERVER_WAIT_TIME = 0.1 #Server Socket Recieve timeout - Raises exception which is ignored, and tries again
        self.CLIENT_MESSAGE_PERIODICITY = 1 #Client message periodicity
            

    # Show details of node
    def show_details(self, param):
        print(getattr(self, param))

    def get_message(self, metric, seqnum):
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

        def update_active_neighbors(message, addr):
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
            with self.lock:
                neighbor_node_name = message["name"]
                current_time = time.time()
                
                # Only update the timestamp if the current time is later
                if neighbor_node_name not in self.neighbors_last_seen or self.neighbors_last_seen[neighbor_node_name] < current_time:
                    self.neighbors_last_seen[neighbor_node_name] = current_time


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

                self.map[self.name] = my_neighbors
  
        def update_rank():
            pass



        """
        Input Args : Message, "from" address
        Function : Updates Active Neigbors, Rank, and Map of the node

        """
        # print(message)

        #Condition to check if it is direct message or forwarded message (Only update active neighbors, if direct message)
        if addr == ((message["host"], message["port"])):
            #Update Active Neighbors
            update_active_neighbors(message, addr)
            update_neighbor_list(message, addr)
        else:
            # print("Updating timestep for indirect neighbor/forwarded message")
            # print("Forwarded message is:", message)
            update_timestamps(message, addr)

        update_map(message, addr)
            
            
    # Start as a server
    def start_server(self):
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

                    #Clean own neighbors in map, if they are inactive
                    if self.name in self.map: 
                        self.map[self.name].pop(neighbor, None)
                #Clean self, if 0 neighbors active
                if self.name in self.map and not self.map[self.name]:
                    self.map.pop(self.name, None)

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