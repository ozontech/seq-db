#!/bin/bash

set -e  # Exit on error
set -o pipefail  # Catch errors in piped commands

# Default values (except POD, which is required)
PORT="8088"
SRC="::data"  # This is an rsync module target, NOT a filesystem path
DST="/data"
INCLUDE_FILE="include.txt"
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
LOG_FILE="transfer_${TIMESTAMP}.log"
POD=""

# Function to display usage
usage() {
    echo "Usage: $0 --pod <pod_address> [--port <port_number>] [--source <rsync_target>] [--destination <destination_path>]"
    echo ""
    echo "  --pod <pod_address>        : REQUIRED. The address of the pod running the rsync daemon."
    echo "  --port <port_number>       : The rsync daemon port (default: $PORT)."
    echo "  --source <rsync_target>    : The rsync module target inside the pod (default: $SRC)."
    echo "                               This should be an rsync module name, NOT a filesystem path."
    echo "                               Example: '::data' refers to the 'data' module in the rsync daemon."
    echo "  --destination <dest_path>  : The local destination path where files will be saved (default: $DST)."
    echo "  --limit <file_count>       : Limit the number of files to transfer (optional)."
    echo "  -h, --help                 : Show this help message and exit."
    echo ""
    echo "Example usage:"
    echo "  $0 --pod my-pod.logging.svc.cluster.local"
    echo "  $0 --pod my-pod.logging.svc.cluster.local --port 9090 --source ::backup --destination /mnt/backup"
    exit 1
}

# Parse command-line arguments
while [[ $# -gt 0 ]]; do
    case "$1" in
        --pod)
            POD="$2"
            shift 2
            ;;
        --port)
            PORT="$2"
            shift 2
            ;;
        --source)
            SRC="$2"
            shift 2
            ;;
        --destination)
            DST="$2"
            shift 2
            ;;
        --limit)
            LIMIT="$2"
            shift 2
            ;;
        -h|--help)
            usage
            ;;
        *)
            echo "Unknown option: $1"
            usage
            ;;
    esac
done

# Ensure required argument is provided
if [[ -z "$POD" ]]; then
    echo "Error: --pod argument is required."
    usage
fi

# Generate file list
echo "Generating file list from $POD$SRC on port $PORT..."
rsync --port "$PORT" "$POD$SRC" | grep -v -e meta -e docs -e '_' | grep index | awk '{print $NF}' | sed 's/.index//g' | awk '{printf "%s.docs\n%s.index\n", $1, $1}' > "$INCLUDE_FILE"

# Apply file limit if specified
if [[ -n "$LIMIT" ]]; then
    cat "$INCLUDE_FILE" | head -n "$LIMIT" > "$INCLUDE_FILE";
fi;

# Start file transfer
echo "Starting file transfer from $POD$SRC to $DST on port $PORT..."
nohup rsync -avh --stats --progress --port "$PORT" --files-from="$INCLUDE_FILE" "$POD$SRC" "$DST" > "$LOG_FILE" &

echo "Transfer started. Logs are being written to $LOG_FILE."