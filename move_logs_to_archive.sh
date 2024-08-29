#!/bin/bash

# Set source and destination directories
SOURCE_DIR="/home/yann/alph-arb-ccxt/Logs"
DEST_DIR="/home/yann/Data/BU/Logs"

# Get today's date in YYYY-MM-DD format
TODAYS_DATE=$(date +%F)

# Loop through all files in the source directory
for file in "$SOURCE_DIR"/*; do
  # Check if the file starts with today's date
  if [[ "$(basename "$file")" != $TODAYS_DATE* ]]; then
    # Move the file to the destination directory
    mv "$file" "$DEST_DIR"
  fi
done

echo "Files moved successfully!"
