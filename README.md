# FastUDP Transfer

A high-performance UDP-based file transfer tool inspired by tsunami-udp, designed for extremely fast file transfers over high-speed networks.

## Features

- UDP-based transfer protocol for maximum throughput
- Selective repeat protocol for reliability
- Configurable window sizes and buffer sizes
- Real-time performance monitoring
- Docker support for easy deployment
- Optimized for high-bandwidth, high-latency networks
- Checksum verification for data integrity

## Requirements

- Python 3.8+
- Network connection with UDP support

## Installation

### Using pip

```bash
pip install -r requirements.txt
```

### Using Docker

```bash
docker build -t fastudp .
```

## Usage

### Server Mode

Run the server to receive files:

```bash
python udp_transfer.py server -o /path/to/output/directory
```

Options:
- `-H, --host`: Host to bind to (default: 0.0.0.0)
- `-p, --port`: Port to bind to (default: 8000)
- `-o, --output-dir`: Directory to save received files (default: current directory)
- `-b, --buffer-size`: Socket buffer size in bytes (default: 16MB)
- `-w, --window-size`: Window size for selective repeat protocol (default: 1024)

Using Docker:
```bash
docker run -p 8000:8000/udp -v /local/path:/data fastudp server -o /data
```

### Client Mode

Send a file to a server:

```bash
python udp_transfer.py client -H server_ip_address -f /path/to/file
```

Options:
- `-H, --host`: Server host (required)
- `-p, --port`: Server port (default: 8000)
- `-f, --file`: File to send (required)
- `-b, --buffer-size`: Socket buffer size in bytes (default: 16MB)
- `-w, --window-size`: Window size for selective repeat protocol (default: 1024)
- `-s, --packet-size`: Maximum packet size (default: 8192)

Using Docker:
```bash
docker run -v /local/path:/data fastudp client -H server_ip_address -f /data/myfile.dat
```

## Performance Tuning

For optimal performance:

1. Increase buffer size for high-bandwidth connections:
   ```bash
   --buffer-size 67108864  # 64MB
   ```

2. Adjust window size based on latency:
   - Higher latency: increase window size
   - Lower latency: smaller window size may suffice
   ```bash
   --window-size 2048  # For high-latency connections
   ```

3. Adjust packet size based on network conditions:
   - Stable network: larger packets
   - Unreliable network: smaller packets
   ```bash
   --packet-size 16384  # For stable, high-speed networks
   ```

## How It Works

FastUDP implements a selective repeat ARQ protocol over UDP:

1. The sender transmits packets within a sliding window
2. The receiver acknowledges correctly received packets
3. Packets with incorrect checksums trigger NACKs for retransmission
4. The window slides as packets are acknowledged
5. Error detection is performed via MD5 checksums

This approach provides reliability while maintaining high throughput, especially on high-bandwidth, high-latency networks (long fat networks).

## Use Cases

- Data center transfers
- Backup and archival operations
- Scientific data distribution
- Media file transfers
- Any scenario requiring high-speed transfer of large files

## Limitations

- Not optimized for extremely lossy networks
- Requires UDP connectivity (may be blocked by some firewalls)
- Performance depends on proper tuning for specific network conditions

## License

MIT License
