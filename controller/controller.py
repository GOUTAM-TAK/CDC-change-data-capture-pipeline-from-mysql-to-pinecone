from service_layer.service import get_all_tables,get_last_watermark,fetch_and_index_data,index_initialize
from utils.config import logger
from flask import jsonify
import traceback
import time
from utils.mysql_connect import connect_to_mysql


def initialize_index():
    try:
       index_initialize()
    except Exception as e:
        print(f"Error initializing Pinecone index: {e}")
        traceback.print_exc()  # Print stack trace for detailed error information
        return jsonify({"error":"Error Initializing pinecone index"}),500
        
def scheduler():
    while True:
      try:
        connection = connect_to_mysql()
        if connection:
            table_names = get_all_tables(connection)
            # Exclude the 'watermark' table
            table_names = [table_name for table_name in table_names if table_name != 'watermark']
            for table_name in table_names:
                last_updated = get_last_watermark(connection, table_name)
                fetch_and_index_data(connection, table_name, last_updated)
      except Exception as e:
            logger.error(f"Error in scheduler: {e}")

      finally:
           if connection and connection.is_connected():
              connection.close()  # Ensure the connection is closed after processing

      time.sleep(300)  # Run every 5 minutes


