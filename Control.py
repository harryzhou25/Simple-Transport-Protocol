import socket
from dataclasses import dataclass


@dataclass
class Control:
    """Control block: parameters for the sender program."""
    host: str  # Hostname or IP address of the receiver
    port: int  # Port number of the receiver
    socket: socket.socket  # Socket for sending/receiving messages
    is_alive: bool = True  # Flag to signal the sender program to terminate
