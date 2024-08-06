from pinecone import Pinecone,ServerlessSpec
from sentence_transformers import SentenceTransformer
import logging
import os

# Configure logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

# Load the sentence transformer model
model = SentenceTransformer('all-MiniLM-L6-v2')

#set environment variable
os.environ['PINECONE_API_KEY'] = "b807b048-2024-47bd-b4d5-c94e5f982ec0"
pinecone_api_key = os.getenv('PINECONE_API_KEY')
index_name = "training-project-vectordb"
spec = ServerlessSpec(region="us-east-1", cloud="aws")

# Initialize Pinecone
pinecone = Pinecone(api_key=pinecone_api_key)



