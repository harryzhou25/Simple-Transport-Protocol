import random
import sys
import time
import socket
import threading
from queue import PriorityQueue


class Receiver:
    """
    Class for STP receiver.
        Input args:
            receiver_port: port number of receiver socket
            receiver_port: port number of sender socket
            max_win: maximum window size
            FileToReceive: address to store received file
        Constant args:
            MOD: MOD number for sequence number
            BUF_SIZE: size of receiving buffer
        Other args:
            st: time of start writing log
            log: log writter
            win_size: current window size
        Counter args:
            dup_ack: number of duplicate acks
            dup_seg: number of duplicate segments
            seg_recv: original number of data received
            data_recv: original length of data received
    """
    def __init__(self):
        self.res_no = -1

        self.buffer = dict()

        self.sender_port = 0
        self.receiver_port = 0
        self.max_win = 0
        self.win_size = 0
        self.wait_time = 2

        self.BUF_SIZE = 2000
        self.MOD = pow(2, 16)

        self.connection_state = True

        self.st = None
        self.log = None

        self.FileToReceive = ''

        self.dup_seg = 0
        self.dup_ack = 0
        self.sge_recv = 0
        self.data_recv = 0

    def parse_wait_time(self, wait_time_str, min_wait_time=1, max_wait_time=60):
        try:
            wait_time = int(wait_time_str)
        except ValueError:
            sys.exit(f"Invalid wait_time argument, must be numerical: {wait_time_str}")

        if not (min_wait_time <= wait_time <= max_wait_time):
            sys.exit(
                f"Invalid wait_time argument, must be between {min_wait_time} and {max_wait_time} seconds: {wait_time_str}")
        return wait_time

    def parse_port(self, port_str, min_port=49152, max_port=65535):
        try:
            port = int(port_str)
        except ValueError:
            sys.exit(f"Invalid port argument, must be numerical: {port_str}")

        if not (min_port <= port <= max_port):
            sys.exit(f"Invalid port argument, must be between {min_port} and {max_port}: {port}")

        return port

    def setup(self, argv):
        if len(argv) != 5:
            sys.exit(f"Usage: {argv[0]} port")
        self.receiver_port = self.parse_port(argv[1])
        self.sender_port = self.parse_port(argv[2])
        self.FileToReceive = argv[3]
        self.max_win = int(argv[4])

    def write_log(self, info=None, mode=False):
        """
        Function is responsible for writing log messages and summary.

        Args:
            info: information to write: (message type, time, package type, sequence number, length of data)
            mode: False for message mode True for summary mode

        Returns:
            None
        """
        if not mode:
            msg = ''
            s = ''
            if info[0] == 0:
                s = 'snd'
            elif info[0] == 1:
                s = 'rcv'
            elif info[0] == 2:
                s = 'drp'
            msg += f'{s:<6}'
            t = round((info[1] - self.st)*1000, 2)
            msg += f'{t:<10}'
            if info[2] == 0:
                s = 'DATA'
            elif info[2] == 1:
                s = 'ACK'
            elif info[2] == 2:
                s = 'SYN'
            elif info[2] == 3:
                s = 'FIN'
            msg += f'{s:<8}'
            msg += f'{str(info[3]):<8}'
            msg += str(info[4])
            msg += '\n'
            self.log.write(msg)
        else:
            items = ['Original data received:', 'Original segments received:',
                     'Dup data segments received:', 'Dup ack segments sent:']
            nums = [self.data_recv, self.sge_recv,
                    self.dup_seg, self.dup_ack]
            for i in range(len(items)):
                s = f'{items[i]:<30}' + str(nums[i]) + '\n'
                self.log.write(s)

    def receive(self):
        """
        'Main' functino of receiver class, responsible for receiving data ,writting data and log.

        Returns:
            None
        """
        st = time.time()
        self.log = open('receiver_log.txt', 'w', encoding='utf-8')
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            s.bind(('', self.receiver_port))  # bind to `port` on all interfaces

            f = open(self.FileToReceive, 'w')
            while True:
                try:
                    buf, addr = s.recvfrom(self.BUF_SIZE)
                    if not addr[1] == self.sender_port:
                        continue
                except socket.timeout:
                    print(f"No data within {self.wait_time} seconds, shutting down.")
                    break
                rec_type = int.from_bytes(buf[:2], byteorder='big')
                msg_no = int.from_bytes(buf[2:4], byteorder='big')
                # print(f'Received {rec_type} {msg_no} {len(buf[4:])}')
                if rec_type == 1:
                    info = (1, time.time(), rec_type, msg_no, 0)
                    self.write_log(info)
                elif rec_type == 3:
                    info = (1, time.time(), rec_type, msg_no, 0)
                    self.write_log(info)
                    s.settimeout(self.wait_time)
                    self.res_no = msg_no + 1
                elif rec_type == 2:
                    self.connection_state = True
                    self.res_no = msg_no + 1
                    self.st = time.time()
                    info = (1, time.time(), rec_type, msg_no, 0)
                    self.write_log(info)
                elif rec_type == 0:
                    info = (1, time.time(), rec_type, msg_no, len(buf[4:]))
                    self.write_log(info)
                    if self.res_no == -1 or msg_no == self.res_no:
                        f.write(str(buf[4:], encoding='utf-8'))
                        self.res_no = (msg_no + + len(buf[4:])) % self.MOD
                        self.data_recv += len(buf[4:])
                        self.sge_recv += 1
                        while len(self.buffer) > 0:
                            if self.res_no in self.buffer:
                                data = self.buffer.pop(self.res_no)
                                self.res_no = (self.res_no + len(data)) % self.MOD
                                f.write(str(data, encoding='utf-8'))
                                self.data_recv += len(data)
                                self.sge_recv += 1
                                self.win_size -= len(data)
                            else:
                                break
                    elif self.res_no < (msg_no + len(buf[4:])) % self.MOD:
                        if self.win_size + len(buf[4:]) <= self.max_win:
                            self.buffer[msg_no] = buf[4:]
                            self.win_size += len(buf[4:])
                    else:
                        self.dup_seg += 1
                        self.dup_ack += 1
                msg_type = 1
                ack = msg_type.to_bytes(2, byteorder='big')
                ack += self.res_no.to_bytes(2, byteorder='big')
                if s.sendto(ack, addr) != len(ack):
                    continue
                info = (0, time.time(), msg_type, self.res_no, 0)
                self.write_log(info)
            self.write_log(mode=True)
            self.log.close()
            f.close()


if __name__ == '__main__':
    receiver = Receiver()
    receiver.setup(sys.argv)
    receiver.receive()
