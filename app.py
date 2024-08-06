from controller.controller import initialize_index, scheduler
from flask import Flask
import threading

app = Flask(__name__)


# Function to run the scheduler in a separate thread
def run_scheduler():
    scheduler_thread = threading.Thread(target=scheduler)
    scheduler_thread.start()
    return scheduler_thread

if __name__ == '__main__':
    initialize_index()
    scheduler_thread =run_scheduler()
    app.run(debug=True, use_reloader=False)
    scheduler_thread.join()