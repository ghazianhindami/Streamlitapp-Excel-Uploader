import os
import sys
import webbrowser
import subprocess

def main():
    script_path = os.path.abspath("Apps/app.py")
    url = "http://localhost:8502"

    # buka browser dulu
    webbrowser.open(url)

    # jalankan Streamlit melalui subprocess
    subprocess.run([
        "streamlit", "run", script_path,
        "--server.headless=true",
        "--server.port=8502",
        "--browser.serverAddress=localhost",
        "--server.fileWatcherType=none"
    ])

if __name__ == "__main__":
    main()