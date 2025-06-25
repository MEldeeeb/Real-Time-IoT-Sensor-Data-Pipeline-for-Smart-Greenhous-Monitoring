#!/bin/bash

echo "Starting hadoop......"
cd C:/hadoop/sbin
./start-all.cmd


# if name node shutdown
# find the process that is using port 9000
netstat -aon | findstr :9000

# kill this process (replace 1234 with the id of process that is listining to the port 90000  )
taskkill /PID 1234 /F


