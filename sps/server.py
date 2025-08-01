import argparse
import logging
from sps import Node

def main():
  parser = argparse.ArgumentParser(description="Launches an SPS Node in sever mode", allow_abbrev=False, formatter_class=argparse.ArgumentDefaultsHelpFormatter)
  parser.add_argument("--server_ips", nargs="+", default=[], help="list of known server ips on the network")
  parser.add_argument("--last_resort_server_ips", nargs="+", default=[], help="list of servers to use as a last resort")
  parser.add_argument("--network_port", type=int, default=8080, help="The port that the netork runs on")
  parser.add_argument("--scan_subnet", action="store_true", help="Automatically discover servers on the local network")
  parser.add_argument("--subnet_mask", default="255.255.254.0", help="The ip mask to use when scanning the subnet for servers")
  parser.add_argument("-v", "--verbose", action="count", default=0, help="Increase output verbosity: -v for INFO, -vv for DEBUG")
  args = parser.parse_args()
  logging.basicConfig(
    level={0: logging.WARNING, 1: logging.INFO, 2: logging.DEBUG}.get(args.verbose, logging.DEBUG),
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%H:%M:%S",
  )

  Node(server=True, **vars(args)).join()
  
if __name__ == "__main__":
  main()
