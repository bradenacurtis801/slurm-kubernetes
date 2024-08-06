#!/bin/bash

MOUNTED_MUNGE_KEY='/tmp/munge/munge.key'
MUNGE_DIR='/etc/munge'

# Function to check if a file exists and is non-empty
check_file_exists() {
  file_path=$1

  echo "Checking if the following path exists: $file_path"
  if [ ! -s "$file_path" ]; then
    echo "Error: $file_path is missing or empty. Please ensure it is properly mounted."
    exit 1
  fi
  echo "file exists!"
}

main() {
  echo "running init script"

  # Check if the munge.key file is mounted
  check_file_exists $MOUNTED_MUNGE_KEY

  # Copy the mounted munge.key to a writable directory
  # Explanation: The munge secret when mounted is read-only, and we need to change ownership and permissions
  # which is not possible directly. Therefore, we copy it to /etc/munge.
  # Better solutions may exist, this is just a quick fix.
  cp $MOUNTED_MUNGE_KEY $MUNGE_DIR/munge.key
  sudo chown munge:munge $MUNGE_DIR/munge.key
  sudo chmod 400 $MUNGE_DIR/munge.key

  # Start MUNGE service
  sudo service munge start
  
  # Start SSH service
  sudo service ssh start

  #tail -f /dev/null
  echo "init script finished"
  echo "starting python application"

  if [ "$SLURM_ROLE" == "master" ]; then
    python3 /App/master/main.py
  elif [ "$SLURM_ROLE" == "worker" ]; then
    python3 /App/worker/main.py
  fi
}

main