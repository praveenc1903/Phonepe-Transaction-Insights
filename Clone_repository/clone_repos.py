import os
import subprocess

REPO_URL = "https://github.com/PhonePe/pulse.git"
DATA_DIR = "pulse-data"

if not os.path.exists(DATA_DIR):
    subprocess.run(["git", "clone", REPO_URL, DATA_DIR])
    print("PhonePe Pulse data cloned")
else:
    print("Pulse data already exists")
