import enum
import logging
import llp
import queue
import struct
import threading
from time import sleep

class SWPType(enum.IntEnum):
    DATA = ord('D')
    ACK = ord('A')

class SWPPacket:
    _PACK_FORMAT = '!BI'
    _HEADER_SIZE = struct.calcsize(_PACK_FORMAT)
    MAX_DATA_SIZE = 1400 # Leaves plenty of space for IP + UDP + SWP header 

    def __init__(self, type, seq_num, data=b''):
        self._type = type
        self._seq_num = seq_num
        self._data = data

    @property
    def type(self):
        return self._type

    @property
    def seq_num(self):
        return self._seq_num
    
    @property
    def data(self):
        return self._data

    def to_bytes(self):
        header = struct.pack(SWPPacket._PACK_FORMAT, self._type.value, 
                self._seq_num)
        return header + self._data
       
    @classmethod
    def from_bytes(cls, raw):
        header = struct.unpack(SWPPacket._PACK_FORMAT,
                raw[:SWPPacket._HEADER_SIZE])
        type = SWPType(header[0])
        seq_num = header[1]
        data = raw[SWPPacket._HEADER_SIZE:]
        return SWPPacket(type, seq_num, data)

    def __str__(self):
        return "%s %d %s" % (self._type.name, self._seq_num, repr(self._data))

class SWPSender:
    _SEND_WINDOW_SIZE = 5
    _TIMEOUT = 1

    def __init__(self, remote_address, loss_probability=0):
        self._llp_endpoint = llp.LLPEndpoint(remote_address=remote_address,
                loss_probability=loss_probability)

        # Start receive thread
        self._recv_thread = threading.Thread(target=self._recv)
        self._recv_thread.start()

        # TODO: Add additional state variables
        self.buffer =[]
        self.sequence_number  = 0
        self.semaphore  =threading.Semaphore(5)
        self.threads = []

    def send(self, data):
        for i in range(0, len(data), SWPPacket.MAX_DATA_SIZE):
            self._send(data[i:i+SWPPacket.MAX_DATA_SIZE])

    def _send(self, data):
        # DEBUG LOGS
        logging.debug("Acquiring the Lock")
        
        # Acquire the Lock
        self.semaphore.acquire()
        
        
        logging.debug(self.semaphore._value)
        
        # Make the Packet
        packet = SWPPacket(SWPType.DATA,self.sequence_number,data)
        
        # DEBUG LOGS
        logging.debug("Buffer BEFORE APPENDING "+str(self.buffer))

        # Append the Packet to Buffer
        self.buffer.append(packet)
        
        # DEBUG LOGS
        logging.debug("Buffer AFTER APPENDING "+str(self.buffer))
        logging.debug("THREAD ARRAY BEFORE APPENDING "+str(self.threads))
        
        # Append the retransmission thread to array
        timer = threading.Timer(SWPSender._TIMEOUT,self._retransmit(packet.seq_num))
        self.threads.append([packet.seq_num,timer])
        
        
        logging.debug("Threads Array After Appending "+str(self.threads))
    
        # Send the Packet
        self._llp_endpoint.send(packet.to_bytes())
        
        # Start The retransmission thread
        timer.start()
        
        # Increment the Sequence number
        self.sequence_number = self.sequence_number + 1
        
        
    def _retransmit(self, seq_num):
        logging.debug("Retransmitting")
        # Retrieve it from Buffer
        packet = [packet for packet in self.buffer if packet.seq_num == seq_num]
        
        # Transmit it thorugh the LLP endpoint
        self._llp_endpoint.send(packet[0].to_bytes())
       
    def _recv(self):
        while True:
            # Receive SWP packet
            raw = self._llp_endpoint.recv()
            if raw is None:
                continue
            packet = SWPPacket.from_bytes(raw)
            logging.debug("Received: %s" % packet)
            # TODO
            if(packet._type == SWPType.ACK):
        
                
                logging.debug("Recived the ACK")
                logging.debug("Packet Sequence number recieved" + str(packet.seq_num))
    

                #DEBUG LOGS
                logging.debug("Buffer BEFORE REMOVING -- RECEVING "+str(self.buffer))
                
                # Remove the Acknowledged Packet From Buffer
                self.buffer = [packets for packets in self.buffer if packets.seq_num != packet.seq_num ]
                
                # DEBUG LOGS
                logging.debug("BUFFER AFTER REMOVING -- RECIEVING  "+str(self.buffer))
                logging.debug("Threads Array before removing"+str(self.threads))
                
                # Shut the thread timer
                thr = [threads for threads in self.threads if threads[0] == packet.seq_num];
                
                # Check if thread exist
                if(len(thr) > 0):
                    # If exist then terminate it
                    thr[0][1].cancel()
                    sleep(2)
                    
                    # Remove the thread from the array
                    self.threads = [thread for thread in self.threads if thread[0] != packet.seq_num]
                    
                    # DEBUG LOGS
                    logging.debug("Threads Array After removing "+str(self.threads))
                    
                    # Release the LOCK
                    self.semaphore.release()
                    logging.debug(self.semaphore._value)

class SWPReceiver:
    _RECV_WINDOW_SIZE = 5

    def __init__(self, local_address, loss_probability=0):
        self._llp_endpoint = llp.LLPEndpoint(local_address=local_address, 
                loss_probability=loss_probability)

        # Received data waiting for application to consume
        self._ready_data = queue.Queue()

        # Start receive thread
        self._recv_thread = threading.Thread(target=self._recv)
        self._recv_thread.start()
        
        # TODO: Add additional state variables


    def recv(self):
        return self._ready_data.get()

    def _recv(self):
        while True:
            # Receive data packet
            raw = self._llp_endpoint.recv()
            packet = SWPPacket.from_bytes(raw)
            logging.debug("Received: %s" % packet)
            
            # TODO

        return
