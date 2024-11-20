import socket as st
import threading as thr
import os
import sys
import json
import time


#Simple Node Structure that stores all details of a particular Node
class Node:

    # init
    def __init__(self, filename):
        # Get details from the conf file
        details = {}
        with open(filename) as f:
            for line in f:
                key, val = line.split("=")
                details[key.strip()] = val.strip()

        # Assign node details
        self.uuid = details['uuid']
        self.name = details['name']
        self.port = int(details['backend_port'])
        self.host = "localhost"
        self.neighbors = {}  
        self.active_neighbors = []
        self.socket = st.socket(st.AF_INET, st.SOCK_DGRAM)
        self.socket.bind((self.host, self.port))

        # Replace set with string or list
        for i in range(int(details['peer_count'])):
            self.neighbors[f"neighbor_{i}"] = details[f'peer_{i}']

        self.running = True
            

    # Show details of node
    def show_details(self, param):
        print(getattr(self, param))

    def get_message(self):
        message = {
            "name": self.name,
            "uuid": self.uuid,
            "port": self.port,
            "neighbors": list(self.neighbors.values())  # Convert neighbors to list
        }
        return message
    
    # Start as a server
    def start_server(self):
        while self.running:
            try:
                message, addr = self.socket.recvfrom(1024)
                message = message.decode('utf-8')

                # If new neighbor, add it with a timestamp
                found = False
                for i, neighbor in enumerate(self.active_neighbors):
                    if neighbor[0] == addr[0] and neighbor[1] == addr[1]:
                        self.active_neighbors[i] = (addr[0], addr[1], time.time())
                        found = True
                        break
                if not found:
                    self.active_neighbors.append((addr[0], addr[1], time.time()))

                # print(self.active_neighbors)
                # print(message)
            except ConnectionResetError as e:
                pass

    def cleanup_neighbors(self, timeout=10):
        while self.running:
            current_time = time.time()
            self.active_neighbors = [
                (ip, port, timestamp) for (ip, port, timestamp) in self.active_neighbors 
                if current_time - timestamp < timeout
            ]
            time.sleep(1)  # Run cleanup every second

    # Start as client
    def start_client(self):
        while self.running:
            try:
                # Create a dictionary message and convert it to a JSON string
                message = json.dumps(self.get_message()).encode('utf-8')
                
                # Send message to all neighbors stored in self.neighbors
                for neighbor in self.neighbors.values():
                    # Split the neighbor string by commas to extract details
                    peer_uuid, host, port, _ = neighbor.split(", ")
                    
                    # Send the message to the neighbor's host and port
                    self.socket.sendto(message, (host, int(port)))
                
                # print(f"Sent message to {len(self.neighbors)} neighbors")
                time.sleep(5)  # Send message every 5 seconds
            except Exception as e:
                pass
    
    def commands(self):
        while True:
            user_input = input("Enter command (type 'exit' to quit): ")
            if user_input == "exit":
                self.running = False
                break
            elif user_input == "uuid":
                print(self.uuid)
            elif user_input == "active_neighbors":
                print(self.active_neighbors)
            else:
                print("Unknown command. Please try again.")

if __name__ == "__main__":
    if len(sys.argv) == 3 and sys.argv[1] == '-c':
        node = Node(sys.argv[2])

        server_thread = thr.Thread(target=node.start_server)
        client_thread = thr.Thread(target=node.start_client)
        cleanup_thread = thr.Thread(target=node.cleanup_neighbors)
        command_thread = thr.Thread(target=node.commands)

        server_thread.start()
        client_thread.start()
        cleanup_thread.start()
        command_thread.start()

        server_thread.join()
        client_thread.join()
        cleanup_thread.join()
        command_thread.join()
