#!/usr/bin/env bash
set -e

# start docker service
/etc/init.d/docker start

# wait for docker to start - danger of infinite loop (deadlock)
while ! pidof containerd >>/dev/null; do
  sleep 0.1
done

# guard
PID="$(cat /var/run/docker.pid)"
if [ -z "$(ps -p $PID -o pid=)" ]; then
  echo "Docker daemon did not start correctly for PID: $PID "
fi
echo "Docker is started with pid: $PID"

# wait
tail -f /dev/null
