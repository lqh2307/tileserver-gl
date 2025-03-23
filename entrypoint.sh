#!/bin/sh

# Run Xvfb
if test -z "$DISPLAY"; then
  echo "Starting Xvfb..."

  Xvfb :99 -screen 0 1920x1080x24 &

  XVFB_PID=$!

  export DISPLAY=:99
fi

# Run nodejs
while true; do
  echo "Starting application..."

  node ./src/main.js "$@"

  EXIT_CODE=$?

  if test "$EXIT_CODE" -eq 0; then
    break
  elif test "$EXIT_CODE" -ne 1; then
    echo "Server exited with code: $EXIT_CODE. Restarting server after 5 seconds..."

    sleep 5
  fi
done

# Stop Xvfb
if test -n "$XVFB_PID"; then
  if ps -p "$XVFB_PID" > /dev/null; then
    echo "Stopping Xvfb..."

    kill "$XVFB_PID"

    wait "$XVFB_PID" 2>/dev/null

    unset DISPLAY
  fi
fi
