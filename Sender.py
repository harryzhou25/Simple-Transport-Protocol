#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
import sys
import time
import random
import socket
import threading
from Control import Control


class Sender:
    """
     Class for STP sender
         Input Args:
            file_addr: the file to send
            flp: forward loss probability
            rlp: reverse loss probability
            rto: value of retransmission timer in milliseconds
            sender_port: port number of sender socket
            receiver_port: port number of receiver socket
        Constant Args:
            MSS: Maximum segment size
            MSN: Maximum sequence number
            MOD: MOD number for sequence number
            ISN: Random initial sequence number
            RECIEVE_BUF_SIZE: Buffer size of receiving thread
            win_size：size of sliding window
        Other Args:
            Log related:
                st: the time of the first successful transmission of SYN
                log: writer of log file
            Reader related:
                reader_size: the size of data that are already read in current sliding window
                    reader_size_mutex: mutex for reader_size
                reader_flag: represents whether reader finished file reading
            Sliding window related:
                seq_no: current sequence number
                le: current left boundary of sliding window
                ri: current right boundary of sliding window
            Retransmission related:
                timer: timer object for timeout retransmission
                    timer_mutex: mutex for timer
                dup_ack: counter of duplicate acks
            Counting related:
                data_sent: Original bytes of data sent
                data_acked: Original bytes of data acked
                pkg_sent: counter for number of sent packages
                retrans_pkg: number of retransmitted packages
                dup_acks: number of duplicated acks
                drop_segs: number of dropped data segments
                drop_acks: number of dropped ack segments

        Data Structures:
            reader_buffer: FIFO python list, buffer for reader
                reader_mutex: mutex for reader_buffer
            buffer: FIFO python list, buffer for sent but not acked data
                buffer_mutex: mutex for buffer
        Message format:
            1 ~ 2 bytes: message type
                DATA = 0, ACK = 1, SYN = 2, FIN = 3
            3 ~ 4 bytes: sequence number
            5 ~ 5+MSS(at most) bytes: Data
        test run command: python sender.py 59606 52513 random1.txt 1000 0 0 0
    """

    def __init__(self, arg):
        self.flp = 0.0
        self.rlp = 0.0
        self.rto = 1000
        self.win_size = 0
        self.file_addr = ''
        self.sender_port = None
        self.receiver_port = None
        self.RECIEVE_BUF_SIZE = 4

        self.MSS = 1000  # Maximum segment size
        self.MSN = pow(2, 16)  # Maximum sequence number
        self.MOD = pow(2, 16)  # MOD number for sequence number

        random.seed()
        self.ISN = random.randint(0, self.MSN)
        self.seq_no = self.ISN

        self.synFlag = False
        self.connectFlag = True

        self.st = time.time()
        self.log = None
        self.log_mutex = False

        self.reader_size = 0
        self.reader_size_mutex = False
        self.reader_buffer = list()
        self.reader_mutex = False
        self.reader_flag = False

        self.buffer = list()
        self.buffer_mutex = False

        self.timer = None
        self.timer_mutex = False
        self.timer_running = False

        self.le = 0
        self.ri = 0

        self.dup_ack = 0

        self.pkg_sent = 0
        self.dup_acks = 0
        self.drop_acks = 0
        self.drop_segs = 0
        self.data_sent = 0
        self.data_acked = 0
        self.retrans_pkg = 0
        self.setup_sender(arg)

    def parse_run_time(self, run_time_str, min_run_time=1, max_run_time=60):
        try:
            run_time = int(run_time_str)
        except ValueError:
            sys.exit(f"Invalid run_time argument, must be numerical: {run_time_str}")

        if not (min_run_time <= run_time <= max_run_time):
            sys.exit(
                f"Invalid run_time argument, must be between {min_run_time} and {max_run_time} seconds: {run_time_str}")

        return run_time

    def parse_port(self, port_str, min_port=49152, max_port=65535):
        try:
            port = int(port_str)
        except ValueError:
            sys.exit(f"Invalid port argument, must be numerical: {port_str}")

        if not (min_port <= port <= max_port):
            sys.exit(f"Invalid port argument, must be between {min_port} and {max_port}: {port}")

        return port

    def parse_file_addr(self, addr):
        if not os.path.exists(addr):
            sys.exit(f"There does not exist such file: {addr}")
        else:
            return addr

    def parse_window_size(self, win):
        if int(win) >= 1000:
            return int(win)
        else:
            sys.exit(f"Max window size does not meet requirement (>= 1000): {win}")

    def parse_rto(self, r):
        if r >= 0:
            return r
        else:
            sys.exit(f"Forward loss probability does not meet requirement (rto should be unsigned): {r}")

    def parse_prob(self, prob, name):
        if 0.0 <= prob <= 1.0:
            return prob
        else:
            sys.exit(f"{name} does not meet requirement (0 <= flp <= 1): {prob}")

    def setup_sender(self, arg):
        """
        Function to initialize constant values of the class.

        Args:
            arg: python list contains input arguments

        Returns:
            None
        """
        self.sender_port = self.parse_port(arg[1])
        self.receiver_port = self.parse_port(arg[2])
        self.file_addr = self.parse_file_addr(arg[3])
        file_size = os.stat(self.file_addr).st_size
        self.win_size = self.parse_window_size(int(arg[4]))
        self.win_size = int(min(file_size / 2, self.win_size))
        self.rto = self.parse_rto(int(arg[5]))
        self.flp = self.parse_prob(float(arg[6]), 'Flp')
        self.rlp = self.parse_prob(float(arg[7]), 'Rlp')
        self.le = self.ISN
        self.ri = self.le + self.win_size
        self.log = open('sender_log.txt', 'w', encoding='utf-8')
        random.seed()

    def setup_socket(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.bind(('127.0.0.1', self.sender_port))
        try:
            sock.connect(('127.0.0.1', self.receiver_port))
        except Exception as e:
            sys.exit(f"Failed to connect to 127.0.0.1:{self.receiver_port}: {e}")
        sock.settimeout(0)
        return sock

    def write_log(self, info=None, mode=False):
        """
        Function to write log messages and summary.

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
            t = round((info[1] - self.st) * 1000, 2)
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
            items = ['Original data sent:', 'Original data acked:', 'Original segments sent:',
                     'Retransmitted segments:', 'Dup acks received:', 'Data segments dropped:',
                     'Ack segments dropped:']
            nums = [self.data_sent, self.data_acked, self.pkg_sent, self.retrans_pkg,
                    self.dup_acks, self.drop_segs, self.drop_acks]
            for i in range(len(items)):
                s = f'{items[i]:<30}' + str(nums[i]) + '\n'
                self.log.write(s)
            self.log.close()

    def reader_thread(self):
        """
        Function for reader thread, which is responsible for reading data into reader buffer from given file

        Returns:
            None
        """
        self.reader_flag = True
        f = open(self.file_addr, 'r')
        data = f.read(self.MSS)
        while data:
            if self.reader_size + len(data) <= self.win_size + self.MSS:
                while self.reader_mutex:
                    pass
                self.reader_mutex = True
                self.reader_buffer.append(data)
                self.reader_mutex = False
                self.reader_size += len(data)
                data = f.read(self.MSS)
        f.close()
        print('Reader Quit')
        self.reader_flag = False

    def timer_thread(self, control):
        """
        Function to retransmit the left most data segment when timer out event occurs.

        Args:
            control: object to send data

        Returns:
            None
        """
        while self.buffer_mutex:
            pass
        self.buffer_mutex = True
        while len(self.buffer) and self.le != self.buffer[0][0]:
            self.buffer.pop(0)
        if len(self.buffer) < 1:
            self.buffer_mutex = False
            return
        seq_no = self.buffer[0][0]
        msg_type = self.buffer[0][2]
        data = self.buffer[0][3]
        self.buffer_mutex = False
        data_seg = data.encode('utf-8')
        msg = msg_type.to_bytes(2, 'big')
        msg += seq_no.to_bytes(2, 'big')
        msg += data_seg
        los = random.random()
        if los > self.flp:
            control.socket.send(msg)
            if not self.synFlag:
                self.synFlag = True
                self.st = time.time()
            info = (0, time.time(), msg_type, self.seq_no, len(data_seg))
            while self.log_mutex:
                pass
            self.log_mutex = True
            self.write_log(info)
            self.log_mutex = False
        else:
            if self.synFlag:
                self.drop_segs += 1
                info = (2, time.time(), msg_type, seq_no, len(data_seg))
                while self.log_mutex:
                    pass
                self.log_mutex = True
                self.write_log(info)
                self.log_mutex = False
        self.timer = threading.Timer(self.rto / 1000, self.timer_thread, args=(control,))
        self.timer.start()
        if msg_type == 0:
            self.retrans_pkg += 1
        if msg_type == 0:
            self.pkg_sent += 1

    def recv_thread(self, control):
        """
        Function for receiving thread, which is responsible for receiving ack, maintain boundary of sliding
        window and fast retransmission

        Args:
            control: object to receive and send packages

        Returns:
            None
        """
        while not self.synFlag:
            pass
        while self.connectFlag:
            try:
                nread = control.socket.recv(self.RECIEVE_BUF_SIZE)
            except BlockingIOError:
                continue  # No data available to read
            except ConnectionRefusedError:
                print(f"recv: connection refused by {control.host}:{control.port}, shutting down...", file=sys.stderr)
                control.is_alive = False
                self.connectFlag = False
                break

            if len(nread) < self.RECIEVE_BUF_SIZE - 1:
                continue

            msg_type = int.from_bytes(nread[:2], byteorder='big')
            seq_no = int.from_bytes(nread[2:4], byteorder='big')

            los = random.random()
            if los < self.rlp:
                self.drop_acks += 1
                info = (2, time.time(), msg_type, seq_no, 0)
                while self.log_mutex:
                    pass
                self.log_mutex = True
                self.write_log(info)
                self.log_mutex = False
                continue

            if self.synFlag:
                info = (1, time.time(), msg_type, seq_no, 0)
                while self.log_mutex:
                    pass
                self.log_mutex = True
                self.write_log(info)
                self.log_mutex = False
            if msg_type == 1:
                # print(f'received {seq_no}')
                while self.buffer_mutex:
                    pass
                self.buffer_mutex = True
                if len(self.buffer) < 1:
                    self.buffer_mutex = False
                    continue
                if (self.ri > self.le and self.le < seq_no <= self.ri) \
                        or (self.le > self.ri and (seq_no > self.le or seq_no <= self.ri)):
                    self.dup_ack = 0  # Reset duplicate ack counter
                    if self.timer_running:
                        # Stop the timer
                        while self.timer_mutex:
                            pass
                        self.timer_mutex = True
                        self.timer_running = False
                        self.timer.cancel()
                        # There are unacked segments -> Restart timer
                        if len(self.buffer) > 1 and not self.timer_running:
                            self.timer = threading.Timer(self.rto / 1000, self.timer_thread, args=(control,))
                            self.timer.start()
                            self.timer_running = True
                        self.timer_mutex = False
                    while len(self.buffer) > 0:
                        if self.buffer[0][1] != seq_no:
                            t = self.buffer.pop(0)
                            self.data_acked += len(t[3])
                            self.data_sent += len(t[3])
                        else:
                            t = self.buffer.pop(0)
                            self.data_acked += len(t[3])
                            self.data_sent += len(t[3])
                            break
                    shift = seq_no - self.le
                    if seq_no < self.le:
                        shift += self.MOD
                    self.le = seq_no
                    self.ri = (self.le + self.win_size) % self.MOD
                    while self.reader_size_mutex:
                        pass
                    self.reader_size_mutex = True
                    self.reader_size -= shift
                    self.reader_size_mutex = False
                    print(f'Reader size shifted {shift}')
                    self.buffer_mutex = False
                else:
                    self.dup_ack += 1
                    self.dup_acks += 1
                    # Fast retransmission
                    if self.dup_ack >= 3:
                        # Drop past packages in buffer
                        if self.data_sent == 0:
                            self.dup_acks -= 1
                        while len(self.buffer) and seq_no != self.buffer[0][0]:
                            t = self.buffer.pop(0)
                        if len(self.buffer) > 0:
                            t = self.buffer[0]
                            msg_type = t[2]
                            data_seg = t[3].encode('utf-8')
                            msg = msg_type.to_bytes(2, 'big')
                            msg += t[0].to_bytes(2, 'big')
                            msg += data_seg
                            los = random.random()
                            if los > self.flp:
                                control.socket.send(msg)
                                print(f'Sent {t[0]}')
                                info = (0, time.time(), msg_type, self.seq_no, len(data_seg))
                                while self.log_mutex:
                                    pass
                                self.log_mutex = True
                                self.write_log(info)
                                self.log_mutex = False
                            else:
                                self.drop_segs += 1
                                info = (2, time.time(), msg_type, seq_no, len(data_seg))
                                while self.log_mutex:
                                    pass
                                self.log_mutex = True
                                self.write_log(info)
                                self.log_mutex = False
                            self.retrans_pkg += 1
                            if msg_type == 0:
                                self.pkg_sent += 1
                        self.dup_ack = 0
                    self.buffer_mutex = False

    def connect(self, control):
        """
        Function to send SYN package and quit after received ack for SYN.

        Args:
            control: object to send data

        Returns:
            None
        """
        msg_type = 2
        msg = msg_type.to_bytes(2, 'big')
        msg += self.seq_no.to_bytes(2, 'big')
        exp_ack = self.seq_no + 1
        los = random.random()
        if los > self.flp:
            self.synFlag = True
            control.socket.send(msg)
            self.st = time.time()
            info = (0, time.time(), msg_type, self.seq_no, 0)
            while self.log_mutex:
                pass
            self.log_mutex = True
            self.write_log(info)
            self.log_mutex = False
        self.connectFlag = True
        while self.buffer_mutex:
            pass
        self.buffer_mutex = True
        self.buffer.append((self.seq_no, exp_ack, 2, ''))
        self.buffer_mutex = False
        if not self.timer_running:
            while self.timer_mutex:
                pass
            self.timer_mutex = True
            self.timer = threading.Timer(self.rto / 1000, self.timer_thread, args=(control,))
            self.timer.start()
            self.timer_running = True
            self.timer_mutex = False
        while self.le < exp_ack:
            pass
        self.seq_no = exp_ack

    def finish(self, control):
        """
        Function to send FIN package and quit after FIN was acked.

        Args:
            control: object to send data

        Returns:
            None
        """
        while self.buffer_mutex:
            pass
        self.buffer_mutex = True
        msg_type = 3
        seq_no = self.seq_no
        msg = msg_type.to_bytes(2, 'big')
        msg += seq_no.to_bytes(2, 'big')
        self.buffer.append((seq_no, seq_no + 1, msg_type, ''))
        los = random.random()
        if los > self.flp:
            print(f'Sent FIN {seq_no}')
            control.socket.send(msg)
            info = (0, time.time(), msg_type, seq_no, 0)
            while self.log_mutex:
                pass
            self.log_mutex = True
            self.write_log(info)
            self.log_mutex = False
        else:
            self.drop_segs += 1
            info = (2, time.time(), msg_type, seq_no, 0)
            while self.log_mutex:
                pass
            self.log_mutex = True
            self.write_log(info)
            self.log_mutex = False
        exp_ack = seq_no + 1
        if not self.timer_running:
            while self.timer_mutex:
                pass
            self.timer_mutex = True
            self.timer = threading.Timer(self.rto / 1000, self.timer_thread, args=(control,))
            self.timer.start()
            self.timer_running = True
            self.timer_mutex = False
        self.buffer_mutex = False
        while self.le < exp_ack:
            pass
        self.connectFlag = False

    def send(self):
        """
        'Main' function of Sender class. Responsible for initializing and starting threads, reading and sending data
        from reader buffer.

        Returns:
            None
        """
        sock = self.setup_socket()

        control = Control('127.0.0.1', self.receiver_port, sock)

        reader = threading.Thread(target=self.reader_thread, args=())
        reader.start()

        receiver = threading.Thread(target=self.recv_thread, args=(control,))
        receiver.start()

        self.connect(control)
        self.seq_no = self.le
        if self.timer_running:
            self.timer.cancel()
        self.timer = threading.Timer(self.rto / 1000, self.timer_thread, args=(control,))
        self.timer_running = False

        # Start transmitting data
        while len(self.reader_buffer) > 0 or len(self.buffer) > 0 or self.reader_flag:
            seq_no = self.seq_no
            while self.reader_mutex:
                pass
            self.reader_mutex = True
            if len(self.reader_buffer) == 0:
                self.reader_mutex = False
                continue
            dataByte = bytes(self.reader_buffer[0], 'utf-8')
            exp_ack = (seq_no + len(dataByte)) % self.MOD
            if (self.le < self.ri < exp_ack) \
                    or (self.le > self.ri) and (self.le > exp_ack > self.ri):
                self.reader_mutex = False
                continue
            data = self.reader_buffer[0]
            self.reader_buffer.pop(0)
            self.reader_mutex = False

            msg_type = 0
            data_seg = data.encode('utf-8')
            msg = msg_type.to_bytes(2, 'big')
            msg += seq_no.to_bytes(2, 'big')
            msg += data_seg
            self.seq_no = exp_ack
            while self.buffer_mutex:
                pass
            self.buffer_mutex = True
            los = random.random()
            if los > self.flp:
                print(f'Sent {seq_no}')
                control.socket.send(msg)
                info = (0, time.time(), msg_type, seq_no, len(data_seg))
                while self.log_mutex:
                    pass
                self.log_mutex = True
                self.write_log(info)
                self.log_mutex = False
            else:
                self.drop_segs += 1
                info = (2, time.time(), msg_type, seq_no, len(data_seg))
                while self.log_mutex:
                    pass
                self.log_mutex = True
                self.write_log(info)
                self.log_mutex = False
            self.pkg_sent += 1
            self.buffer.append((seq_no, exp_ack, msg_type, data))
            if not self.timer_running:
                self.timer = threading.Timer(self.rto / 1000, self.timer_thread, args=(control,))
                self.timer.start()
                self.timer_running = True
            self.buffer_mutex = False
        self.finish(control)
        receiver.join()
        self.timer.cancel()
        control.is_alive = False
        reader.join()
        while self.log_mutex:
            pass
        self.log_mutex = True
        self.write_log(mode=True)
        self.log_mutex = False


if __name__ == '__main__':
    sender = Sender(sys.argv)
    sender.send()

# D:
# cd UNSW\COMP9331\Assignment1\v1.0.3_0415
# python sender.py 51030 52513 random1.txt 10000 1 0.0 0.0
