#!/bin/bash

XVFB_DISPLAY=99
XVFB_SCREEN="1920x1080x24"
XVFB_LOCK="/tmp/.X${XVFB_DISPLAY}-lock"
XVFB_SOCKET="/tmp/.X11-unix/X${XVFB_DISPLAY}"

OS="$(uname -s)"

if [[ "$OS" == "Linux" ]]; then
  while true; do
    echo "Checking display on Linux..."

    if command -v xdpyinfo >/dev/null 2>&1 && xdpyinfo >/dev/null 2>&1; then
      echo "System display detected, running app without Xvfb!"

      node ./src/main.js "$@"
      EXIT_CODE=$?
    else
      echo "No system display, starting Xvfb..."

      export DISPLAY=:$XVFB_DISPLAY

      pkill Xvfb || true
      rm -f "$XVFB_LOCK" "$XVFB_SOCKET"

      Xvfb :$XVFB_DISPLAY -screen 0 $XVFB_SCREEN -nolisten tcp &
      XVFB_PID=$!

      started=false
      for i in $(seq 1 10); do
        if xdpyinfo -display :$XVFB_DISPLAY >/dev/null 2>&1; then
          started=true

          break
        fi

        sleep 0.5
      done

      if [ "$started" != "true" ]; then
        echo "Xvfb failed to start, retrying after 5s..."

        kill "$XVFB_PID" 2>/dev/null || true
        rm -f "$XVFB_LOCK" "$XVFB_SOCKET"

        sleep 5

        continue
      fi

      node ./src/main.js "$@"
      EXIT_CODE=$?

      kill "$XVFB_PID" 2>/dev/null || true
      rm -f "$XVFB_LOCK" "$XVFB_SOCKET"
    fi

    [ "$EXIT_CODE" -eq 0 ] && exit 0

    echo "App exited with exit code $EXIT_CODE, restarting after 5s..."

    sleep 5
  done
elif [[ "$OS" == "MINGW"* ]] || [[ "$OS" == "CYGWIN"* ]] || [[ "$OS" == "MSYS"* ]]; then
  echo "Running on Windows, assuming system display is available!"

  while true; do
    node ./src/main.js "$@"
    EXIT_CODE=$?

    [ "$EXIT_CODE" -eq 0 ] && exit 0

    echo "App exited with exit code $EXIT_CODE, restarting after 5s..."

    sleep 5
  done
else
  echo "Unsupported OS: $OS. Exitted!"

  exit 1
fi
