from elasticsearch import Elasticsearch


elastic_host="http://localhost:9200"

# Connect to Elasticsearch
es = Elasticsearch(hosts=elastic_host)

# Define the index settings and mappings (you can customize these based on your data)
index_name = 'mastodonuno'
mapping = {
        'mappings': {
        'properties': {
            # Careful with the format for the data, "yyyy-MM-dd'T'HH:mm:ss.SSSZ"
            "created_at": {"type": "date","format": "yyyy-MM-dd'T'HH:mm:ss.SSSZ"},
            "content": {"type": "text","fielddata": True}
            # Add more fields and their types as needed
        }
    }
}

# Create the index
response = es.indices.create(index=index_name, body=mapping)

# Check if the index creation was successful
if 'acknowledged' in response and response['acknowledged']:
    print(f"Index '{index_name}' created successfully.")
else:
    print(f"Failed to create index '{index_name}': {response}")
