# Query the indeces
import requests

def get_all_indices():
    # Elasticsearch server URL
    # es_url = "http://localhost:9200"
    es_url = "http://elasticsearch:9200"

    try:
        # Send a GET request to Elasticsearch to retrieve all indices
        response = requests.get(f"{es_url}/_cat/indices?v")

        # Check the response status code
        if response.status_code == 200:
            # Parse the response content as text
            response_text = response.text

            # Split the response text into lines to separate each index information
            index_lines = response_text.splitlines()

            # Skip the header line (index names)
            index_lines = index_lines[1:]

            # Extract and print the index names
            for line in index_lines:
                index_name = line.split()[2]
                print("########>{}".format(index_name))

        else:
            print(f"###########>Failed to retrieve indices from Elasticsearch. Status Code: {response.status_code}")
    
    except requests.exceptions.RequestException as e:
        print(f"~~~~~~~~~~~~~~~~~~>Error: {e}")


get_all_indices()