#!/bin/sh

set -e

XVFB_DISPLAY=99
XVFB_SCREEN="1920x1080x24"
XVFB_LOCK="/tmp/.X${XVFB_DISPLAY}-lock"
XVFB_SOCKET="/tmp/.X11-unix/X${XVFB_DISPLAY}"

while true; do
  echo "Checking display..."

  if xdpyinfo >/dev/null 2>&1; then
    echo "System display detected, running app without Xvfb"

    node ./src/main.js "$@"

    EXIT_CODE=$?
  else
    echo "No system display, starting Xvfb"

    export DISPLAY=:$XVFB_DISPLAY

    pkill Xvfb || true
    rm -f "$XVFB_LOCK" "$XVFB_SOCKET"

    Xvfb :$XVFB_DISPLAY -screen 0 $XVFB_SCREEN -nolisten tcp &

    XVFB_PID=$!

    for i in $(seq 1 20); do
      xdpyinfo -display :$XVFB_DISPLAY >/dev/null 2>&1 && break

      sleep 0.5
    done

    if ! xdpyinfo -display :$XVFB_DISPLAY >/dev/null 2>&1; then
      echo "Xvfb failed to start, retrying..."

      kill "$XVFB_PID" 2>/dev/null || true

      rm -f "$XVFB_LOCK" "$XVFB_SOCKET"

      sleep 2

      continue
    fi

    node ./src/main.js "$@"

    EXIT_CODE=$?

    kill "$XVFB_PID" 2>/dev/null || true
    rm -f "$XVFB_LOCK" "$XVFB_SOCKET"
  fi

  [ "$EXIT_CODE" -eq 0 ] && exit 0

  echo "App exited with $EXIT_CODE, restarting in 5s..."

  sleep 5
done
