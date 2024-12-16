# Project2_18741_CMU
Project 2 - Content Server - A Systems Programming Project as part of CMU's 18741 Computer Networks Course taught by Dr. Swarun Kumar

# Overlay Network for Content Distribution

## Description

This project simulates an overlay network that connects nodes replicating common content, implementing a simple link-state routing protocol. It allows efficient transport by using distance metrics to calculate the shortest paths between nodes. The project is designed for educational purposes in understanding distributed systems and networking.

The code supports the following functionalities:
- Discovering and maintaining active neighbors.
- Sharing network topology across nodes.
- Calculating shortest paths using Dijkstra's algorithm.
- Handling dynamic addition of neighbors.
- Periodically cleaning up inactive neighbors.

## Requirements

- **Python Version**: Python 3.9 or above
- **Libraries**: 
  - `socket`
  - `threading`
  - `sys`
  - `json`
  - `time`
  - `re`
  - `numpy`
  
- **Configuration Files**: A `.conf` file specifying node details and initial neighbors is required to start a node. 

## Usage

### Command-Line Interface
Run the script from the command line:
```bash
python3 content_server.py -c /path/to/config_file.conf

