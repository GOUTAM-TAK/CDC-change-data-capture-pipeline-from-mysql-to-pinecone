from utils.config import  logger,pinecone,index_name,spec,model
from utils.embeddings_utils import SentenceTransformerEmbedding
from langchain_pinecone import PineconeVectorStore
import json
import decimal
import uuid
import pandas as pd
from datetime import datetime,date
from utils.util import get_primary_key_column
     
def get_all_tables(connection):
    try:
      with connection.cursor() as cursor:
        cursor.execute("SHOW TABLES")
        tables = cursor.fetchall()
        return [table[0] for table in tables]
    except Exception as e:
       logger.error(f"error in get_last_watermark : {e}")
       raise
    
def get_last_watermark(connection,table_name):
    try:

       with connection.cursor() as cursor:
          cursor.execute("SELECT last_updated FROM watermark WHERE table_name = %s", (table_name,))
          result = cursor.fetchone()
          if result:
            return result[0]
          else:
            return None
    except Exception as e:
        logger.error(f"error in get_last_watermark : {e}")
        raise
        
def update_watermark(connection,table_name, last_updated):
    try:
      with connection.cursor() as cursor:
        cursor.execute("""
            INSERT INTO watermark (table_name, last_updated) 
            VALUES (%s, %s)
            ON DUPLICATE KEY UPDATE last_updated = VALUES(last_updated)
        """, (table_name, last_updated))
        connection.commit()
    except Exception as e:
        logger.error(f"error in update_watermark : {e}")
        raise

def fetch_and_index_data(connection, table_name, last_updated):
    try:
      cursor = connection.cursor(dictionary=True)
    
      if last_updated:
        query = f"SELECT * FROM {table_name} WHERE change_datetime > %s"
        cursor.execute(query, (last_updated,))
      else:
        query = f"SELECT * FROM {table_name}"
        cursor.execute(query)
    
      records = cursor.fetchall()
      print(f"successfully fetch data from {table_name}")
      cursor.close()

      primary_column = get_primary_key_column(connection,table_name)

      if records:
        all_data = []
        df = pd.DataFrame(records)
        for index, row in df.iterrows():
            row_data = row.to_dict()
            all_data.append({"data": row_data})
        
        process_and_index_data(all_data, table_name)
        new_last_updated = max(record['change_datetime'] for record in records)
        update_watermark(connection, table_name, new_last_updated)
        print(f"successfully updated watermark table with table name : {table_name} new last update : {new_last_updated}")
      else:
            print(f"No new records found in table: {table_name}")
    except Exception as e:
        logger.error(f"error in fetch_index_data : {e}")
        raise

def index_initialize():
    try:
        # Check if the index already exists
        if index_name not in pinecone.list_indexes().names():

            # Create the index with the specified parameters
            pinecone.create_index(
                name=index_name,
                dimension=384,
                metric="cosine",
                spec=spec
            )
            print("Index created successfully.")
        global pinecone_index
        pinecone_index = pinecone.Index(index_name)
        print(pinecone_index.describe_index_stats())
    except Exception as e:
       logger.error(f"error in index_initialize : {e}")
       raise

#process data and store into vector database
def process_and_index_data(data, table_name):
    try:
        if not data:
            logger.warning("No data to index.")
            return
        
        chunk_size = 100  # Number of rows per chunk
        documents = []
        metadata_list = []
        chunk = []

        # Prepare chunks of data
        for i, d in enumerate(data):
            if isinstance(d, dict) and "data" in d:
                data_string = json.dumps(d["data"], default=json_serialize)
                source = table_name

                # Add current row to the chunk
                chunk.append(data_string)

                # If the chunk is full, process it
                if len(chunk) == chunk_size:
                    # Generate a UUID for the chunk
                    chunk_uuid = str(uuid.uuid4())
                    combined_data_string = " ".join(chunk)

                    # Use the table name and UUID to create a unique identifier
                    chunk_unique_id = f"{table_name}#{chunk_uuid}"

                    documents.append(combined_data_string)
                    metadata_list.append({"source": table_name, "id": chunk_unique_id, "text": combined_data_string})

                    # Reset chunk
                    chunk = []

        # Handle the last chunk if it's not empty
        if chunk:
            chunk_uuid = str(uuid.uuid4())
            combined_data_string = " ".join(chunk)
            chunk_unique_id = f"{table_name}#{chunk_uuid}"

            documents.append(combined_data_string)
            metadata_list.append({"source": table_name, "id": chunk_unique_id, "text": combined_data_string})

        # Creating or updating the vector store
        embedding = SentenceTransformerEmbedding(model)

        # Process in batches
        batch_size = 10  # Number of chunks to process in each batch
        for i in range(0, len(documents), batch_size):
            batch_documents = documents[i:i + batch_size]
            batch_metadata = metadata_list[i:i + batch_size]

            # Create embeddings for the batch
            embeddings = embedding.embed_documents(batch_documents)

            # Prepare data for upsert
            upsert_data = [
                {"id": meta["id"], "values": emb, "metadata": meta}
                for emb, meta in zip(embeddings, batch_metadata)
            ]

            # Insert or update records in Pinecone
            pinecone_index.upsert(vectors=upsert_data, namespace='task1')

            print(f"Batch {i // batch_size + 1} indexed successfully in Pinecone.")

    except Exception as e:
        logger.error(f"Error processing and indexing data: {e}")
        raise



# Define custom JSON serializer for objects not serializable by default JSON encoder
def json_serialize(obj):
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    if isinstance(obj, decimal.Decimal):
        return float(obj)
    raise TypeError(f"Type {type(obj)} not serializable")