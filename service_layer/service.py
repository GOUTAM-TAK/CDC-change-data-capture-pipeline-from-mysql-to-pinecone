from utils.config import  logger,pinecone,index_name,spec,model
from utils.embeddings_utils import SentenceTransformerEmbedding
from langchain_pinecone import PineconeVectorStore
import json
import decimal
import pandas as pd
from datetime import datetime,date
from utils.util import get_primary_key_column
"""def fetch_all_tables_data(connection):
    try:
        #connection = connect_to_mysql()
        cursor = connection.cursor()
        cursor.execute("SHOW TABLES")
        tables = cursor.fetchall()

        all_data = [] 

        for table in tables:
            table_name=table[0]
            query=f"SELECT * FROM {table_name}"
            df = pd.read_sql(query, connection)
          
            for index, row in df.iterrows():
                row_data = row.to_dict()
                all_data.append({"data": row_data, "source":f"MySQL table : {table_name}"})

        cursor.close
        print("successfully fetch data from mysql database")
        connection.close()
        return all_data
    except Exception as e:
        logger.error(f"Error fetching data from MySQL: {e}")
        return jsonify({"detail":"Error fetching data from database"}),500"""
     
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
            all_data.append({"data": row_data, "source": table_name})
        
        process_and_index_data(all_data, primary_column, table_name, delete_old=True)
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
def process_and_index_data(data, primary_column,table_name, delete_old=False):
    try:
        if not data:
            logger.warning("No data to index.")
            return
        
        chunk_size = 1000  # Define your chunk size
        documents = []
        metadata_list = []
        ids_to_delete = []

        # Prepare documents and metadata
        for d in data:
            if isinstance(d, dict) and "data" in d:
                data_string = json.dumps(d["data"], default=json_serialize)
                source = d.get("source", "unknown")
                #table_name = source.split(":")[1].strip()  # Extract table_name from source

                pk_col = primary_column
                if not pk_col:
                    logger.error(f"Primary key column for table {table_name} not found.")
                    continue

                id_value = d["data"].get(pk_col)
                unique_id = f"{table_name}_{id_value}"  # Generate unique ID

                if delete_old:
                    ids_to_delete.append(unique_id)

                documents.append(data_string)
                metadata_list.append({"source": source, "id": unique_id, "text":data_string})

        # Deleting old records if needed
        if delete_old and ids_to_delete:
            namespace = 'task1'  # Use default namespace
            pinecone_index.delete(ids=ids_to_delete, namespace=namespace)
            print(f"Deleted old records from Pinecone: {ids_to_delete}")

        # Creating or updating the vector store
        embedding = SentenceTransformerEmbedding(model)

        #docsearch = PineconeVectorStore(index_name=index_name,embedding=embedding)
         # Create embeddings for documents
        embeddings = [embedding.embed_documents(document) for document in documents]

        # Prepare data for upsert
        upsert_data = [
            {"id": meta["id"], "values": emb, "metadata": meta}
            for emb, meta in zip(embeddings, metadata_list)
        ]
        # Insert or update records
        pinecone_index.upsert(vectors=upsert_data,namespace='task1')

        print("Data indexed successfully in Pinecone.")

    except Exception as e:
        logger.error(f"Error processing and indexing data: {e}")
        raise

"""def process_and_index_data(data):
    try:
        if not data:
            logger.warning("No data to index.")
            return
        
        chunk_size = 1000  # Define your chunk size
        documents = []
        metadata_list = []

        for d in data:
            if isinstance(d, dict) and "data" in d:
                data_string = json.dumps(d["data"], default=json_serialize)
                source = d.get("source", "unknown")
            else:
                data_string = str(d)  # Handle cases where d might be a string directly
                source = "unknown"  # Default source for string data

            chunks = [data_string[i:i+chunk_size] for i in range(0, len(data_string), chunk_size)]
            for chunk in chunks:
                documents.append(chunk)
                metadata_list.append({"source": source})

        # Create a custom embedding object
        embedding = SentenceTransformerEmbedding(model)

        # Create or update the vector store
        docsearch = PineconeVectorStore.from_texts(
            texts=documents,
            embedding=embedding,  # Pass the embedding object
            metadatas=metadata_list,
            index_name=index_name
        )
        print("Data indexed successfully in Pinecone.")

    except Exception as e:
        logger.error(f"Error processing and indexing data: {e}")
        raise"""

# Define custom JSON serializer for objects not serializable by default JSON encoder
def json_serialize(obj):
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    if isinstance(obj, decimal.Decimal):
        return float(obj)
    raise TypeError(f"Type {type(obj)} not serializable")