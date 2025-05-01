#!/usr/bin/env python3
"""
High-Speed UDP File Transfer Implementation
Similar to tsunami-udp functionality with client/server architecture
"""

import argparse
import asyncio
import hashlib
import logging
import os
import socket
import struct
import sys
import time
from dataclasses import dataclass
from enum import Enum
from typing import Dict, List, Optional, Tuple, Union

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("fast_udp")

# Constants
MAX_PACKET_SIZE = 8192  # Default UDP packet size
DEFAULT_BUFFER_SIZE = 16 * 1024 * 1024  # 16MB buffer
DEFAULT_WINDOW_SIZE = 1024  # Window size for selective repeat
DEFAULT_PORT = 8000
TIMEOUT_SECONDS = 5.0
HEADER_SIZE = 24  # bytes


class PacketType(Enum):
    """Packet types for protocol communication"""
    HANDSHAKE = 0
    DATA = 1
    ACK = 2
    NACK = 3
    COMPLETE = 4
    ERROR = 5


@dataclass
class Packet:
    """Packet structure for the UDP protocol"""
    type: PacketType
    seq_num: int
    total_packets: int
    data_size: int
    checksum: bytes
    data: bytes = b''
    
    @classmethod
    def from_bytes(cls, packet_bytes: bytes) -> 'Packet':
        """Deserialize a packet from bytes"""
        header = packet_bytes[:HEADER_SIZE]
        ptype, seq_num, total_packets, data_size = struct.unpack("!IIII", header[:16])
        checksum = header[16:HEADER_SIZE]
        data = packet_bytes[HEADER_SIZE:HEADER_SIZE + data_size]
        
        return cls(
            type=PacketType(ptype),
            seq_num=seq_num,
            total_packets=total_packets,
            data_size=data_size,
            checksum=checksum,
            data=data
        )
    
    def to_bytes(self) -> bytes:
        """Serialize packet to bytes"""
        header = struct.pack(
            "!IIII", 
            self.type.value, 
            self.seq_num, 
            self.total_packets, 
            self.data_size
        ) + self.checksum
        
        return header + self.data
    
    @staticmethod
    def calculate_checksum(data: bytes) -> bytes:
        """Calculate MD5 checksum for data integrity"""
        return hashlib.md5(data).digest()


class TransferStats:
    """Track transfer statistics"""
    def __init__(self):
        self.start_time = time.time()
        self.bytes_sent = 0
        self.bytes_received = 0
        self.packets_sent = 0
        self.packets_received = 0
        self.retransmits = 0
    
    def get_throughput(self) -> float:
        """Calculate throughput in Mbps"""
        elapsed = time.time() - self.start_time
        if elapsed == 0:
            return 0
        
        # For server, use bytes received; for client, use bytes sent
        bytes_transferred = max(self.bytes_sent, self.bytes_received)
        return (bytes_transferred * 8) / (elapsed * 1_000_000)  # Convert to Mbps
    
    def print_stats(self, is_server: bool = False):
        """Print transfer statistics"""
        elapsed = time.time() - self.start_time
        role = "Server" if is_server else "Client"
        transfer_type = "received" if is_server else "sent"
        
        bytes_transferred = self.bytes_received if is_server else self.bytes_sent
        mb_transferred = bytes_transferred / (1024 * 1024)
        
        logger.info(f"{role} Statistics:")
        logger.info(f"  Total {transfer_type}: {mb_transferred:.2f} MB")
        logger.info(f"  Elapsed time: {elapsed:.2f} seconds")
        logger.info(f"  Throughput: {self.get_throughput():.2f} Mbps")
        logger.info(f"  Packets {transfer_type}: {self.packets_received if is_server else self.packets_sent}")
        logger.info(f"  Retransmissions: {self.retransmits}")


class UDPSocket:
    """Enhanced UDP socket with reliability features"""
    def __init__(self, 
                 host: str, 
                 port: int, 
                 buffer_size: int = DEFAULT_BUFFER_SIZE, 
                 window_size: int = DEFAULT_WINDOW_SIZE):
        self.host = host
        self.port = port
        self.buffer_size = buffer_size
        self.window_size = window_size
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, buffer_size)
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, buffer_size)
        
        # Set socket timeout
        self.socket.settimeout(TIMEOUT_SECONDS)
        
        # Stats tracking
        self.stats = TransferStats()
    
    def bind(self):
        """Bind socket to address (for server)"""
        self.socket.bind((self.host, self.port))
        logger.info(f"Socket bound to {self.host}:{self.port}")
    
    def send_packet(self, packet: Packet, addr: Optional[Tuple[str, int]] = None) -> int:
        """Send a packet to the specified address"""
        target_addr = addr if addr else (self.host, self.port)
        packet_bytes = packet.to_bytes()
        sent = self.socket.sendto(packet_bytes, target_addr)
        
        self.stats.bytes_sent += sent
        self.stats.packets_sent += 1
        
        return sent
    
    def receive_packet(self, buffer_size: int = MAX_PACKET_SIZE) -> Tuple[Packet, Tuple[str, int]]:
        """Receive a packet"""
        try:
            data, addr = self.socket.recvfrom(buffer_size)
            packet = Packet.from_bytes(data)
            
            self.stats.bytes_received += len(data)
            self.stats.packets_received += 1
            
            return packet, addr
        except socket.timeout:
            raise TimeoutError("Socket receive timed out")
    
    def close(self):
        """Close the socket"""
        self.socket.close()


class Server:
    """UDP file transfer server"""
    def __init__(self, 
                 host: str = "0.0.0.0", 
                 port: int = DEFAULT_PORT,
                 output_dir: str = ".",
                 buffer_size: int = DEFAULT_BUFFER_SIZE,
                 window_size: int = DEFAULT_WINDOW_SIZE):
        self.host = host
        self.port = port
        self.output_dir = output_dir
        self.socket = UDPSocket(host, port, buffer_size, window_size)
        self.window_size = window_size
        self.received_packets: Dict[int, Packet] = {}
        self.next_expected_seq = 0
        self.client_addr = None
        self.current_file = None
        self.current_filename = None
        self.total_packets = 0
        
        # Ensure output directory exists
        os.makedirs(output_dir, exist_ok=True)
    
    async def start(self):
        """Start the server"""
        self.socket.bind()
        logger.info(f"Server started on {self.host}:{self.port}")
        
        try:
            while True:
                await self.handle_client()
        except KeyboardInterrupt:
            logger.info("Server shutting down")
        finally:
            self.socket.close()
    
    async def handle_client(self):
        """Handle a client connection"""
        try:
            # Wait for handshake
            packet, addr = self.socket.receive_packet()
            if packet.type != PacketType.HANDSHAKE:
                logger.error(f"Expected handshake, got {packet.type}")
                return
            
            self.client_addr = addr
            filename = packet.data.decode('utf-8')
            self.current_filename = os.path.join(self.output_dir, os.path.basename(filename))
            self.total_packets = packet.total_packets
            
            logger.info(f"Handshake from {addr}, receiving file: {self.current_filename}")
            logger.info(f"Expected total packets: {self.total_packets}")
            
            # Send ACK for handshake
            ack_packet = Packet(
                type=PacketType.ACK,
                seq_num=0,
                total_packets=self.total_packets,
                data_size=0,
                checksum=b'\x00' * 8
            )
            self.socket.send_packet(ack_packet, addr)
            
            # Open file for writing
            self.current_file = open(self.current_filename, 'wb')
            
            # Reset sequence tracking
            self.received_packets = {}
            self.next_expected_seq = 0
            
            # Start receiving data
            await self.receive_file()
            
        except Exception as e:
            logger.error(f"Error handling client: {e}")
            if self.current_file:
                self.current_file.close()
    
    async def receive_file(self):
        """Receive a file from the client using selective repeat"""
        logger.info("Starting file transfer")
        self.socket.stats = TransferStats()  # Reset stats
        
        while len(self.received_packets) < self.total_packets:
            try:
                packet, addr = self.socket.receive_packet()
                
                if packet.type == PacketType.COMPLETE:
                    logger.info("Received COMPLETE signal from client")
                    break
                
                if packet.type != PacketType.DATA:
                    logger.warning(f"Unexpected packet type: {packet.type}")
                    continue
                
                # Verify checksum
                calculated_checksum = Packet.calculate_checksum(packet.data)
                if calculated_checksum != packet.checksum:
                    logger.warning(f"Checksum mismatch for packet {packet.seq_num}")
                    # Send NACK
                    nack_packet = Packet(
                        type=PacketType.NACK,
                        seq_num=packet.seq_num,
                        total_packets=self.total_packets,
                        data_size=0,
                        checksum=b'\x00' * 8
                    )
                    self.socket.send_packet(nack_packet, addr)
                    continue
                
                # Store the packet
                self.received_packets[packet.seq_num] = packet
                
                # Process in-order packets
                self.process_in_order_packets()
                
                # Send ACK
                ack_packet = Packet(
                    type=PacketType.ACK,
                    seq_num=packet.seq_num,
                    total_packets=self.total_packets,
                    data_size=0,
                    checksum=b'\x00' * 8
                )
                self.socket.send_packet(ack_packet, addr)
                
                # Periodically log progress
                if packet.seq_num % 1000 == 0 or packet.seq_num + 1 == self.total_packets:
                    progress = (len(self.received_packets) / self.total_packets) * 100
                    logger.info(f"Progress: {progress:.2f}% ({len(self.received_packets)}/{self.total_packets})")
                    logger.info(f"Current throughput: {self.socket.stats.get_throughput():.2f} Mbps")
            
            except TimeoutError:
                logger.warning("Timeout waiting for packets")
                # Could implement additional recovery here
        
        # Close the file
        if self.current_file:
            self.current_file.close()
            logger.info(f"File transfer complete: {self.current_filename}")
            
            # Send completion acknowledgment
            complete_packet = Packet(
                type=PacketType.COMPLETE,
                seq_num=0,
                total_packets=self.total_packets,
                data_size=0,
                checksum=b'\x00' * 8
            )
            self.socket.send_packet(complete_packet, self.client_addr)
            
            # Print stats
            self.socket.stats.print_stats(is_server=True)
    
    def process_in_order_packets(self):
        """Process packets that are in order"""
        while self.next_expected_seq in self.received_packets:
            packet = self.received_packets[self.next_expected_seq]
            self.current_file.write(packet.data)
            del self.received_packets[self.next_expected_seq]
            self.next_expected_seq += 1


class Client:
    """UDP file transfer client"""
    def __init__(self, 
                 server_host: str,
                 server_port: int = DEFAULT_PORT,
                 buffer_size: int = DEFAULT_BUFFER_SIZE,
                 window_size: int = DEFAULT_WINDOW_SIZE,
                 max_packet_size: int = MAX_PACKET_SIZE):
        self.server_host = server_host
        self.server_port = server_port
        self.socket = UDPSocket("0.0.0.0", 0, buffer_size, window_size)
        self.window_size = window_size
        self.max_packet_size = max_packet_size
        self.server_addr = (server_host, server_port)
        self.window: Dict[int, Packet] = {}
        self.acked_packets: List[int] = []
        self.nacked_packets: List[int] = []
        self.base_seq_num = 0
        self.next_seq_num = 0
    
    async def send_file(self, filepath: str):
        """Send a file to the server"""
        if not os.path.exists(filepath):
            raise FileNotFoundError(f"File not found: {filepath}")
        
        filesize = os.path.getsize(filepath)
        filename = os.path.basename(filepath)
        
        logger.info(f"Sending file: {filename} ({filesize / (1024*1024):.2f} MB)")
        
        # Calculate total packets
        self.total_packets = (filesize + self.max_packet_size - HEADER_SIZE - 1) // (self.max_packet_size - HEADER_SIZE)
        logger.info(f"Total packets to send: {self.total_packets}")
        
        # Send handshake
        handshake_packet = Packet(
            type=PacketType.HANDSHAKE,
            seq_num=0,
            total_packets=self.total_packets,
            data_size=len(filename.encode('utf-8')),
            checksum=b'\x00' * 8,
            data=filename.encode('utf-8')
        )
        self.socket.send_packet(handshake_packet, self.server_addr)
        
        # Wait for handshake ACK
        try:
            packet, addr = self.socket.receive_packet()
            if packet.type != PacketType.ACK or packet.seq_num != 0:
                logger.error("Handshake failed")
                return False
            logger.info("Handshake successful")
        except TimeoutError:
            logger.error("Handshake timed out")
            return False
        
        # Reset tracking variables
        self.window = {}
        self.acked_packets = []
        self.nacked_packets = []
        self.base_seq_num = 0
        self.next_seq_num = 0
        
        # Start sending data
        self.socket.stats = TransferStats()  # Reset stats
        
        with open(filepath, 'rb') as file:
            # Start receiver task
            receiver_task = asyncio.create_task(self.receiver_loop())
            
            try:
                while True:
                    # If window is not full and we have more data to send
                    while len(self.window) < self.window_size and self.next_seq_num < self.total_packets:
                        # Calculate how much data to read
                        data_size = self.max_packet_size - HEADER_SIZE
                        data = file.read(data_size)
                        
                        if not data:
                            break  # End of file
                        
                        # Create and send packet
                        checksum = Packet.calculate_checksum(data)
                        packet = Packet(
                            type=PacketType.DATA,
                            seq_num=self.next_seq_num,
                            total_packets=self.total_packets,
                            data_size=len(data),
                            checksum=checksum,
                            data=data
                        )
                        
                        self.window[self.next_seq_num] = packet
                        self.socket.send_packet(packet, self.server_addr)
                        self.next_seq_num += 1
                        
                        # Periodically log progress
                        if self.next_seq_num % 1000 == 0 or self.next_seq_num == self.total_packets:
                            progress = (self.next_seq_num / self.total_packets) * 100
                            logger.info(f"Sent: {progress:.2f}% ({self.next_seq_num}/{self.total_packets})")
                            logger.info(f"Current throughput: {self.socket.stats.get_throughput():.2f} Mbps")
                    
                    # Process any NACKed packets
                    if self.nacked_packets:
                        seq_num = self.nacked_packets.pop(0)
                        if seq_num in self.window:
                            packet = self.window[seq_num]
                            logger.debug(f"Resending packet {seq_num}")
                            self.socket.send_packet(packet, self.server_addr)
                            self.socket.stats.retransmits += 1
                    
                    # Check if we've sent and received ACKs for all packets
                    if self.base_seq_num >= self.total_packets:
                        logger.info("All packets sent and acknowledged")
                        break
                    
                    # Small pause to prevent CPU hogging
                    await asyncio.sleep(0.001)
                
                # Send COMPLETE signal
                complete_packet = Packet(
                    type=PacketType.COMPLETE,
                    seq_num=0,
                    total_packets=self.total_packets,
                    data_size=0,
                    checksum=b'\x00' * 8
                )
                self.socket.send_packet(complete_packet, self.server_addr)
                
                # Wait for server's COMPLETE acknowledgment
                try:
                    packet, addr = self.socket.receive_packet()
                    if packet.type == PacketType.COMPLETE:
                        logger.info("Server acknowledged completion")
                except TimeoutError:
                    logger.warning("Timeout waiting for server completion acknowledgment")
                
                # Cancel receiver task
                receiver_task.cancel()
                try:
                    await receiver_task
                except asyncio.CancelledError:
                    pass
                
                # Print stats
                self.socket.stats.print_stats()
                return True
                
            except Exception as e:
                logger.error(f"Error during file transfer: {e}")
                receiver_task.cancel()
                try:
                    await receiver_task
                except asyncio.CancelledError:
                    pass
                return False
    
    async def receiver_loop(self):
        """Loop to receive ACKs and NACKs from the server"""
        try:
            while True:
                try:
                    packet, addr = self.socket.receive_packet()
                    
                    if packet.type == PacketType.ACK:
                        # Mark packet as acknowledged
                        seq_num = packet.seq_num
                        if seq_num in self.window:
                            self.acked_packets.append(seq_num)
                            del self.window[seq_num]
                            
                            # Update base sequence number
                            while self.base_seq_num < self.total_packets and self.base_seq_num not in self.window:
                                self.base_seq_num += 1
                    
                    elif packet.type == PacketType.NACK:
                        # Mark packet for retransmission
                        seq_num = packet.seq_num
                        if seq_num in self.window and seq_num not in self.nacked_packets:
                            self.nacked_packets.append(seq_num)
                    
                    elif packet.type == PacketType.COMPLETE:
                        logger.info("Received completion confirmation from server")
                        break
                
                except TimeoutError:
                    # Implement timeout logic if needed
                    # For simple implementation, we just continue
                    pass
                
                # Small pause to prevent CPU hogging
                await asyncio.sleep(0.001)
                
        except asyncio.CancelledError:
            logger.debug("Receiver loop cancelled")
            raise


async def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description="High-Speed UDP File Transfer")
    subparsers = parser.add_subparsers(dest="mode", help="Mode of operation")
    
    # Server options
    server_parser = subparsers.add_parser("server", help="Run in server mode")
    server_parser.add_argument("-H", "--host", default="0.0.0.0", help="Host to bind to")
    server_parser.add_argument("-p", "--port", type=int, default=DEFAULT_PORT, help="Port to bind to")
    server_parser.add_argument("-o", "--output-dir", default=".", help="Output directory for received files")
    server_parser.add_argument("-b", "--buffer-size", type=int, default=DEFAULT_BUFFER_SIZE, help="Socket buffer size in bytes")
    server_parser.add_argument("-w", "--window-size", type=int, default=DEFAULT_WINDOW_SIZE, help="Window size for selective repeat")
    
    # Client options
    client_parser = subparsers.add_parser("client", help="Run in client mode")
    client_parser.add_argument("-H", "--host", required=True, help="Server host")
    client_parser.add_argument("-p", "--port", type=int, default=DEFAULT_PORT, help="Server port")
    client_parser.add_argument("-f", "--file", required=True, help="File to send")
    client_parser.add_argument("-b", "--buffer-size", type=int, default=DEFAULT_BUFFER_SIZE, help="Socket buffer size in bytes")
    client_parser.add_argument("-w", "--window-size", type=int, default=DEFAULT_WINDOW_SIZE, help="Window size for selective repeat")
    client_parser.add_argument("-s", "--packet-size", type=int, default=MAX_PACKET_SIZE, help="Maximum packet size")
    
    args = parser.parse_args()
    
    if args.mode == "server":
        server = Server(
            host=args.host,
            port=args.port,
            output_dir=args.output_dir,
            buffer_size=args.buffer_size,
            window_size=args.window_size
        )
        await server.start()
    
    elif args.mode == "client":
        client = Client(
            server_host=args.host,
            server_port=args.port,
            buffer_size=args.buffer_size,
            window_size=args.window_size,
            max_packet_size=args.packet_size
        )
        success = await client.send_file(args.file)
        sys.exit(0 if success else 1)
    
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
