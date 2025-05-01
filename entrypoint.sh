#!/bin/bash
set -e

# Print version and info
echo "FastUDP Transfer Tool"
echo "--------------------"

# Handle server/client modes and pass remaining arguments
if [ "$1" = "server" ]; then
    echo "Running in SERVER mode"
    exec python /app/udp_transfer.py server "${@:2}"
elif [ "$1" = "client" ]; then
    echo "Running in CLIENT mode"
    exec python /app/udp_transfer.py client "${@:2}"
else
    # If no valid mode specified, show help
    echo "Usage: $(basename $0) [server|client] [options]"
    echo "For more information:"
    exec python /app/udp_transfer.py --help
fi
