from sps import Node
import argparse

def main():
  parser = argparse.ArgumentParser(description="Launches an SPS Node in sever mode", formatter_class=argparse.ArgumentDefaultsHelpFormatter)
  parser.add_argument("--server_ips", nargs="+", default=[], help="list of known server ips on the network")
  parser.add_argument("--last_resort_server_ips", nargs="+", default=[], help="list of servers to use as a last resort")
  parser.add_argument("--network_port", type=int, default=8080, help="The port that the netork runs on")
  parser.add_argument("--scan_subnet", action="store_true", help="Automatically discover servers on the local network")
  parser.add_argument("--subnet_mask", default="255.255.254.0", help="The ip mask to use when scanning the subnet for servers")
  args = parser.parse_args()

  node = Node(server=True, **vars(args))
  node.join()
  
if __name__ == "__main__":
  main()
