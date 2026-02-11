# run.py
import os
import subprocess
import time
import sys
from datetime import datetime
from watchdog.observers.polling import PollingObserver as Observer
from watchdog.events import FileSystemEventHandler
import signal
import socket
import re

# ------------------------------
FLASK_APP = "main.py"
WATCH_EXTENSIONS = (".py", ".html", ".js", ".css")
DEBOUNCE_SECONDS = 1
IGNORE_FOLDERS = ["__pycache__"]
FLASK_PORT = 5000
CHECK_PORT_INTERVAL = 0.3
# ------------------------------

def log(msg):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[Ahmed] [{timestamp}] {msg}")

# ---------- STUCK PROCESS SCANNER ----------
def scan_and_kill_stuck(port=FLASK_PORT):
    log(f"Scanning for stuck processes on port {port}...")
    spinner = ['|', '/', '-', '\\']
    for i in range(6):  # animate scanning
        sys.stdout.write(f"\rScanning {spinner[i % len(spinner)]} ")
        sys.stdout.flush()
        time.sleep(0.1)
    sys.stdout.write("\rScanning... done ✅\n")

    # Run lsof to find PIDs
    try:
        result = subprocess.run(f"lsof -t -i:{port}", shell=True, capture_output=True, text=True)
        pids = result.stdout.strip().splitlines()
        if not pids or pids == ['']:
            log(f"No stuck processes found on port {port}")
            return
        log(f"Found stuck PIDs: {', '.join(pids)}")
        # Kill them
        for pid in pids:
            sys.stdout.write(f"Killing PID {pid}... ")
            sys.stdout.flush()
            os.system(f"kill -9 {pid}")
            time.sleep(0.1)
            print("Done ✅")
    except Exception as e:
        log(f"Error scanning/killing stuck processes: {e}")

def is_port_free(port):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        try:
            s.bind(("0.0.0.0", port))
            return True
        except OSError:
            return False

def wait_for_port_free(port, timeout=5):
    start_time = time.time()
    while not is_port_free(port):
        scan_and_kill_stuck(port)
        time.sleep(CHECK_PORT_INTERVAL)
        if time.time() - start_time > timeout:
            log(f"Warning: port {port} still busy after {timeout}s, will try dynamic port")
            return False
    return True

# ---------- WATCHDOG HANDLER ----------
class RestartHandler(FileSystemEventHandler):
    def __init__(self, port):
        self.process = None
        self.last_restart = 0
        self.flask_port = port
        wait_for_port_free(self.flask_port)
        self.start_process()

    def start_process(self):
        if self.process:
            log(f"Stopping previous process PID {self.process.pid}...")
            try:
                self.process.send_signal(signal.SIGTERM)
                self.process.wait()
            except Exception as e:
                log(f"Failed to stop process: {e}")

        wait_for_port_free(self.flask_port)

        # Start Flask
        self.process = subprocess.Popen(["python", FLASK_APP], env={**os.environ, "FLASK_RUN_PORT": str(self.flask_port)})
        log(f"Started {FLASK_APP} with PID {self.process.pid} on port {self.flask_port}")

    def on_any_event(self, event):
        if event.is_directory:
            return
        path = os.path.abspath(event.src_path)
        for folder in IGNORE_FOLDERS:
            folder_path = os.path.join(os.path.dirname(os.path.abspath(FLASK_APP)), folder)
            if os.path.commonpath([folder_path, path]) == folder_path:
                return
        if not path.endswith(WATCH_EXTENSIONS):
            return
        now = time.time()
        if now - self.last_restart < DEBOUNCE_SECONDS:
            return
        self.last_restart = now
        log(f"Detected change in {path}, restarting Flask...")
        self.start_process()

# ---------- MAIN ----------
if __name__ == "__main__":
    project_path = os.path.dirname(os.path.abspath(FLASK_APP))
    port_to_use = FLASK_PORT
    # Scan & kill stuck processes before first start
    wait_for_port_free(port_to_use)

    event_handler = RestartHandler(port_to_use)
    observer = Observer()
    observer.schedule(event_handler, path=project_path, recursive=True)
    observer.start()
    log(f"Watchdog started. Monitoring {WATCH_EXTENSIONS} in {project_path} (ignoring {IGNORE_FOLDERS})...")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        log("Shutting down watchdog...")
        observer.stop()
        if event_handler.process:
            event_handler.process.terminate()
    observer.join()