import collections
import copy
import inspect
import ipaddress
import logging
import os
import pickle
import random
import socket
import struct
import threading
import time
import uuid
from collections import namedtuple
from types import SimpleNamespace as MSG
from types import SimpleNamespace as AddressState
from types import SimpleNamespace as Connection
from types import SimpleNamespace as Route

Address = namedtuple("Address", ["mac", "pid"])

class Node:

  def __init__(self, server_ips=[], last_resort_server_ips=[], network_port=None, server=False, scan_subnet=False, subnet_mask="255.255.255.0"):
    self.logger = logging.getLogger(__name__)

    self.address = Address(mac=uuid.getnode(), pid=os.getpid())
    self.server_ips = server_ips
    self.last_resort_server_ips = last_resort_server_ips
    self.network_port = network_port
    self.address_expiration_s = 2#10

    # variables locked under state_lock
    self.addresses = {} # Address -> AddressState(timestamp, expiration_time)
    self.subscriptions = collections.defaultdict(set) # topic -> set of Addresses
    self.connections = {} # Address -> Connection(sock, send_lock, last_resort, routes). Route(min_steps, min_connection_addresses)
    self.callbacks = collections.defaultdict(list) # topic -> list of callbacks
    self.addresses[self.address] = AddressState(timestamp=time.time(), expiration_time=None)
    self.state_rlock = threading.RLock()


    logging.info(f"self.address = {self.address.mac}-{self.address.pid}")

    self.subscribe("subscriptions", self._subscriptions_callback)

    threading.Thread(target=self._run_address_expiror, kwargs={"period_s":20}, daemon=True).start()
    if server_ips or last_resort_server_ips:
      threading.Thread(target=self._run_server_connector, kwargs={"period_s":5}, daemon=True).start()
    if server:
      threading.Thread(target=self._run_connection_listener, daemon=True).start()
    if scan_subnet:
      threading.Thread(target=self._run_subnet_scanner, kwargs={"subnet_mask":subnet_mask, "period_s":60}, daemon=True).start()

  def publish(self, data, topic=None):
    if topic is None:
      if hasattr(data, "__class__"):
        topic = data.__class__.__name__ # infer topic from object's class
      else:
        raise TypeError("Argument 'topic' is None and data is not an object from which topic can be infered from class name")
    self._route_message(MSG(topic=topic, data=pickle.dumps(data), addresses=self.subscriptions[topic]), None)

  def subscribe(self, topic, callback):
    if not isinstance(topic, str):
      if isinstance(topic, type):
        topic = topic.__name__ # topic was a class
      else:
        raise TypeError("Argument 'topic' must be a string or class.")
    with self.state_rlock:
      self.callbacks[topic].append(callback)
      if self.address not in self.subscriptions[topic]:
        self.subscriptions[topic].add(self.address)
        self.addresses[self.address].timestamp = time.time()
        self._propagate_subscriptions(self.connections.keys())

  def unsubscribe(self, topic):
    if not isinstance(topic, str):
      if isinstance(topic, type):
        topic = topic.__name__ # topic was a class
      else:
        raise TypeError("Argument 'topic' must be a string or class.")
    with self.state_rlock:
      self.callbacks[topic][:] = []
      if self.address in self.subscriptions[topic]:
        self.subscriptions[topic].remove(self.address)
        self.addresses[self.address].timestamp = time.time()
        self._propagate_subscriptions(self.connections.keys())

  def join(self):
    assert threading.current_thread() == threading.main_thread(), "Error: Join from non-main thread. Only join from the main thread."
    try:
      [thread.join() for thread in threading.enumerate() if thread is not threading.current_thread()]
    except KeyboardInterrupt:
      pass # could shutdown all threads with a shutdown_event.set() followed by another "self.join()", then re-raise the KeyboardInterrupt for the client

  ## callbacks ##

  """Update subscription state and rebroadcast if necessary"""
  def _subscriptions_callback(self, data):
    logging.debug("_subscriptions_callback: here")
    (sender_address, addresses, subscriptions, routes) = data

    # below identifies if there is updated information to be sent to the sender (update_sender)
    # and or forwarded to the other connections (state_updated)
    update_sender = False # send an updated subscriptions message back to sender
    state_updated = False # send an updated subscriptions message to all other connections

    with self.state_rlock:
      # check for a change in distance to any address
      if routes != self.connections[sender_address].routes:
        curr_routes = self._get_routes()
        self.connections[sender_address].routes = routes
        new_routes = self._get_routes()
        if curr_routes != new_routes:
          logging.debug(f"_subscriptions_callback: change in distance, {new_routes}")
          update_sender = True
          state_updated = True
    
      # check for a change in addresses
      if addresses != self.addresses:
        # update addresses
        for address in (set(addresses.keys()) | set(self.addresses.keys())):
          if address in addresses and address in self.addresses and addresses[address] == self.addresses[address]:
            continue

          # address is new, or address.timestamp is newer, or timestamp is same and expiration added
          if ((address not in self.addresses) or
              ((address in self.addresses and address in addresses) and
               (addresses[address].timestamp > self.addresses[address].timestamp or
                (addresses[address].timestamp == self.addresses[address].timestamp and
                 addresses[address].expiration_time != None and
                 (self.addresses[address].expiration_time == None or addresses[address].expiration_time < self.addresses[address].expiration_time))))):
            self.addresses[address] = addresses[address]
            for topic in (set(self.subscriptions.keys()) | set(subscriptions.keys())):
              if address in self.subscriptions[topic] and address not in subscriptions[topic]:
                self.subscriptions[topic].remove(address)
              if address in subscriptions[topic] and address not in self.subscriptions[topic]:
                self.subscriptions[topic].add(address)
            state_updated = True # for this address, the sender had more current data
            logging.debug(f"_subscriptions_callback: sender ({sender_address}) has more accurate info about {address}")
          else:
            update_sender = True # for this address, this node has more current data
            logging.debug(f"_subscriptions_callback: I ({self.address}) have more accurate info about {address} than sender ({sender_address})")

      # clear this node's expiration if set
      if self.addresses[self.address].expiration_time != None:
        self.addresses[self.address].expiration_time = None
        self.addresses[self.address].timestamp = time.time()
        state_updated = True
        update_sender = True
        logging.debug("_subscriptions_callback: clear this node's expiration in addresses")

    # update sender
    if update_sender:
      self._propagate_subscriptions([sender_address])

    # update other connections
    if state_updated:
      self._propagate_subscriptions(list(set(self.connections.keys())-{sender_address}))

    if not update_sender and not state_updated:
      logging.debug("_subscriptions_callback: nothing new")
      
  ## thread functions ##

  """Periodically remove addresses and subscriptions if address has expired"""
  def _run_address_expiror(self, period_s):
    while True:
      logging.debug("RUNNING EXPIROR")
      logging.debug(self.addresses)
      now = time.time()
      with self.state_rlock:
        for address, address_state in list(self.addresses.items()):
          if address_state.expiration_time is not None and now > address_state.expiration_time:
            logging.debug(f"address {address} expired")
            del self.addresses[address]
            for topic in self.subscriptions.keys():
              if address in self.subscriptions[topic]:
                self.subscriptions[topic].remove(address)
      time.sleep(period_s)

  """Periodically connect to any supplied ips if connection was lost"""
  def _run_server_connector(self, period_s):
    while True:
      connection_ips = [connection.connection_ip for connection in self.connections.copy().values()]
      for server_ip in set(self.server_ips) | set(self.last_resort_server_ips):
        if server_ip not in connection_ips:
          threading.Thread(target=self._run_connection, kwargs={"server_ip":server_ip}, daemon=True).start()
      time.sleep(period_s)

  """Listen on port and spawn _run_connection on connections"""
  def _run_connection_listener(self):
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_socket.bind(("0.0.0.0", self.network_port))
    server_socket.listen()
    while True:
      client_socket, _ = server_socket.accept()
      threading.Thread(target=self._run_connection, kwargs={"sock":client_socket}, daemon=True).start()

  """Initiate and process all read messages from a connection"""
  def _run_connection(self, server_ip=None, sock=None):
    logging.debug("running connection")
    if server_ip:
      sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
      try:
        sock.connect((server_ip, self.network_port))
      except socket.error as e:
        return
      
    send_lock = threading.Lock()

    # get connection_ip
    try:
      connection_ip = sock.getpeername()[0]
    except OSError:
      return # socket closed
    
    # send an intro message ("data" is not pickled first here here)
    success = self._send_msg(MSG(topic="intro", data=(self.address), addresses=["you"]), sock, send_lock)
    if not success:
      return # socket closed

    # read intro message
    msg = self._read_msg(sock)
    if not msg:
      return # socket closed
    assert msg.topic == "intro"

    # add connection
    connection_address = msg.data
    if connection_address in self.connections:
      sock.close()
      return # these nodes are already connected
    last_resort = (connection_ip in self.last_resort_server_ips)
    with self.state_rlock:
      self.connections[connection_address] = Connection(sock=sock, send_lock=send_lock, last_resort=last_resort, connection_ip=connection_ip, routes={})

    # send subscriptions
    self._propagate_subscriptions([connection_address])

    # read messages
    while True:
      msg = self._read_msg(sock)
      if not msg: # connection closed
        self._close_connection(connection_address)
        break
      self._route_message(msg, transmitter_address=connection_address)

  """Periodically scan subnet trying to connect to servers"""
  def _run_subnet_scanner(self, subnet_mask, period_s):
    while True:
      # get this device's ip
      with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
        sock.connect(("8.8.8.8", 1)) # UDP so no actual connection to this dummy ip+port
        this_ip, _ = sock.getsockname()

      # get server ips to try
      network = ipaddress.IPv4Network((this_ip, subnet_mask), strict=False)
      connection_ips = [connection.connection_ip for connection in self.connections.values()]
      server_ips = [str(ip) for ip in network.hosts() if ip != this_ip and ip not in self.server_ips+self.last_resort_server_ips+connection_ips]

      # spawn _run_connection's to attempt connection
      for server_ip in server_ips:
        threading.Thread(target=self._run_connection, kwargs={"server_ip":server_ip}, daemon=True).start()

      time.sleep(period_s)

  ## helpers ##

  """Route messages to the appropriate callbacks and connections"""
  def _route_message(self, msg, transmitter_address):
    msg.addresses = copy.copy(msg.addresses)
    
    # handle the special case in which msg.addresses = "you"
    if msg.addresses == "you":
      for callback in self.callbacks[msg.topic].copy():
        callback(pickle.loads(msg.data))
      return

    # deliver to this node
    if self.address in msg.addresses:
      for callback in self.callbacks[msg.topic].copy():
        callback(pickle.loads(msg.data))
      msg.addresses.remove(self.address)

    # aggregate remaining addresses by best connection for delivery
    routes = self._get_routes()
    send_to_connection = collections.defaultdict(list) # connection_address -> [dst_address,]
    for address in msg.addresses.copy():
      if address in routes:
        send_to_connection[random.sample(routes[address].min_connection_addresses, 1)[0]].append(address)
        msg.addresses.remove(address)

    # send out to the appropriate connection
    for connection_address, dst_addresses in send_to_connection.items():
      new_msg = copy.copy(msg)
      new_msg.addresses = dst_addresses
      success = self._send_msg(new_msg, self.connections[connection_address].sock, self.connections[connection_address].send_lock)
      if not success:
        self._close_connection(connection_address)

    # log remaining msg.addresses as failed deliveries
    if len(msg.addresses) > 0:
      self.logger.debug(f"No route found for msg {msg.topic} to subscribers {list(str(address.mac)+'-'+str(address.pid) for address in msg.addresses)}.")
      
  def _get_routes(self):
    routes = {} # address -> route. Route(min_steps, min_connection_addresses = [Address, ])
    for address in self.addresses.copy():
      if address == self.address:
        routes[address] = Route(min_steps=0, min_connection_addresses=[self.address])
      else:
        for connection_address, connection in self.connections.copy().items():
          if (address in connection.routes and any(address != self.address for address in connection.routes[address].min_connection_addresses)):
            connection_steps = connection.routes[address].min_steps + (100 if connection.last_resort else 0)
            if address not in routes or connection_steps+1 < routes[address].min_steps:
              routes[address] = Route(min_steps=connection_steps+1, min_connection_addresses=[connection_address])
            elif connection_steps+1 == routes[address].min_steps:
              routes[address].min_connection_addresses.append(connection_address)
    return routes

  def _propagate_subscriptions(self, connection_addresses):
    routes = self._get_routes()
    # crash on the next line, "Runtime error: dictionary changed size during iteration". seems to be pointing at picke.dumps
    msg = MSG(topic="subscriptions", data=pickle.dumps((self.address, self.addresses, self.subscriptions, routes)), addresses="you")
    for address in list(connection_addresses).copy():
      logging.info(f"_propagate_subscriptions: propagate to {address}")
      success = self._send_msg(msg, self.connections[address].sock, self.connections[address].send_lock)
      if not success:
        self._close_connection(address)

  def _close_connection(self, connection_address):
    logging.debug("CLOSING CONNECTION")
    with self.state_rlock:
      if connection_address in self.connections:
        logging.debug("removing connection")

        # remove this connection
        logging.debug(f"_run_connection(): connection_lost, deleting from connections: {connection_address}")
        del self.connections[connection_address]

        # set expiration for all addresses only reachable via this connection
        expiration_time = time.time()+self.address_expiration_s
        routes = self._get_routes() # address -> route
        for address in self.addresses:
          if address not in routes:
            self.addresses[address].expiration_time = expiration_time

        # propagate the updated expiration(s)
        self._propagate_subscriptions(self.connections.keys())
        
      else:
        logging.debug("connection already removed")

  def _send_msg(self, msg, sock, send_lock):
    data = pickle.dumps(msg)
    length = struct.pack("!I", len(data)) # 4-byte unsigned int, network byte order
    try:
      with send_lock:
        sock.sendall(length+data)
    except Exception:
      return False
    return True

  def _read_msg(self, sock):
    # read length
    raw_length = self._readall(sock, 4)
    if not raw_length:
      return None # socket is closed
    length, = struct.unpack("!I", raw_length)

    # read message
    data = self._readall(sock, length)
    if not data:
      return None # socket is closed
    msg = pickle.loads(data)

    return msg

  def _readall(self, sock, n_bytes):
    data = b""
    while len(data) < n_bytes:
      try:
        packet = sock.recv(n_bytes - len(data))
      except socket.error as e:
        return None # socket closed
      if not packet:
        return None # socket closed
      data += packet
    return data
  
