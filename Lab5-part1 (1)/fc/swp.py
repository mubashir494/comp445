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
        
        # Intialize the thread 
        timer = threading.Timer(SWPSender._TIMEOUT,lambda :self._retransmit(packet.seq_num))

        # Append it to thread Array        
        self.threads.append([packet.seq_num,timer])
        
        logging.debug("Threads Array After Appending "+str(self.threads))
        
        # Send the Packet
        self._llp_endpoint.send(packet.to_bytes())
        
        logging.debug("Timer object "+str(timer))
        # Start the timer
        timer.start()
        
        # Increment the sequence Number
        self.sequence_number = self.sequence_number + 1
        
        
    def _retransmit(self, seq_num):
        logging.debug("Retransmitting")
        logging.debug("SEQ NUM "+str(seq_num))
        # Retrieve it from Buffer
        packet = [packet for packet in self.buffer if packet.seq_num == seq_num]
        logging.debug(str(packet))
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
                element = [el for el in self.buffer if el.seq_num == packet.seq_num]
               
                if(len(element) > 0) :
                                       
                    logging.debug("Recived the ACK")
                    logging.debug("Packet Sequence number recieved" + str(packet.seq_num))
                    
                    thr = [threads for threads in self.threads if threads[0] == packet.seq_num];
                    # Check if thread Exist
                    if (len(thr) > 0):
                         
                        # If exist then terminate it    
                        thr[0][1].cancel()
                            
                        # DEBUG LOGS
                        logging.debug("Threads Array before removing"+str(self.threads))    
                        
                        # Remove the thread from the array
                        self.threads = [thread for thread in self.threads if thread[0] != packet.seq_num]        
                        
                        # DEBUG LOGS
                        logging.debug("Threads Array After removing "+str(self.threads))
                        
                        #DEBUG LOGS
                        logging.debug("Buffer BEFORE REMOVING -- RECEVING "+str(self.buffer))
                        
                        # Remove the Acknowledged Packet From Buffer
                        self.buffer = [packets for packets in self.buffer if packets.seq_num != packet.seq_num ]
                        
                        # DEBUG LOGS
                        logging.debug("BUFFER AFTER REMOVING -- RECIEVING  "+str(self.buffer))
                            
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
        self.buffer = []
        self.expected_seq_num = 0 
        
        


    def recv(self):
        return self._ready_data.get()

    def ack_packet(self,packet):
        self._llp_endpoint.send(packet.to_bytes())
        
    
    def _recv(self):
        while True:
            # Receive data packet
            raw = self._llp_endpoint.recv()
            packet = SWPPacket.from_bytes(raw)
            logging.debug("Received: %s" % packet)
            # Verify the Type of Packet
            if(packet.type == SWPType.DATA):
                # If the Packets is what its expected
                if(packet.seq_num == self.expected_seq_num):                
                    packet = [next_packet for next_packet in self.buffer if next_packet.seq_num == packet.seq_num]
                   
                    if(len(packet) > 0):
                        # Queue the Packet
                        self._ready_data.queue(packet)      
                        # Remove it from the Buffer
                        self.buffer = [next_packet for next_packet in self.buffer if next_packet.seq_num != packet.seq_num]          
                   
                    else:
                        # Queue the Packet
                        self._ready_data.queue(packet)
                    
                    # Increment the Expected Sequence Number
                    self.expected_seq_num += 1
                    
                    # Check if any consective segments exist in buffer
                    buffer_expected_seq = self.expected_seq_num
                    
                    # Traverse through the Buffer
                    while(True):    
                        intermediate = [next_packet for next_packet in self.buffer if next_packet.seq_num == buffer_expected_seq]
                        if(len(intermediate) > 0):
                            self._ready_data(intermediate[0])
                            # Increment the Expected sequence number
                            buffer_expected_seq += 1
                            self.expected_seq_num = buffer_expected_seq
                        else:
                            break
                    
                # If packet Sequence Number is greater then the expected sequence number
                elif (packet.seq_num > self.expected_seq_num ) :
                    # Check If It exist in Buffer
                    packet = [next_packet for next_packet in self.buffer if next_packet.seq_num == packet.seq_num]
                    # If it Does not exist then Append it to the Buffer
                    if(len(packet) == 0):
                        self.buffer.append(packet)
                    # Else Do nothing
                
                
                # If Packet is less then Expected
                else :
                    pass
            
                
                # Send Acknowledgment of Highest Acknowledged Segment
                pack = SWPPacket(SWPType.ACK,self.expected_seq_num - 1)
                self.ack_packet(pack)
                                    
                    
            # TODO

        return
