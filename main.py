import streamlit.web.bootstrap as bootstrap
import os
import sys
import webbrowser

def main():
    script_path = os.path.abspath("Apps/app.py")
    sys.argv = [
        "streamlit",
        "run",
        script_path,
        "--server.headless=true",
        "--server.port=8501",
        "--browser.serverAddress=localhost",
        "--server.fileWatcherType=none"
    ]
    bootstrap.run(script_path, False, [], {})
    webbrowser.open("http://localhost:8501")

if __name__ == "__main__":
    main()