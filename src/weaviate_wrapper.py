import weaviate
from weaviate.classes.config import Configure
from pprint import pprint
import json
import os
import logging
from typing import List, Dict, Any, Union

# --- Logging Setup ---
# Define a directory for logs
log_directory = "logs"
if not os.path.exists(log_directory):
    os.makedirs(log_directory)

# Get the name of the current script to use in the log file name
# Note: When this code is imported as a module, __file__ will refer to the module's path.
# For more granular logging, consider using logging.getLogger(__name__) within the class.
current_file_name_base = os.path.basename(__file__).split('.')[0]
log_file_path = os.path.join(log_directory, f"{current_file_name_base}.log")

# Configure basic logging
logging.basicConfig(
    filename=log_file_path,
    level=logging.INFO,
    format="%(asctime)s %(levelname)s: %(message)s",
)
logging.info(f"'{current_file_name_base}' module started.")

class WeaviateClient:
    """
    A class to encapsulate Weaviate client interactions, including collection management,
    data addition, and semantic search.
    """

    def __init__(self, port: int = 8080, ollama_api_endpoint: str = "http://host.docker.internal:11434"):
        """
        Initializes the Weaviate client.

        Args:
            port (int): The port where the Weaviate instance is running.
            ollama_api_endpoint (str): The API endpoint for the Ollama text embedding model.
                                       Defaults to "http://host.docker.internal:11434" for Docker environments.
                                       Other options include "http://localhost:11434" or "http://ollama:11434".
        """
        self.port = port
        self.ollama_api_endpoint = ollama_api_endpoint
        self.client = None # Will be connected as needed

        logging.info(f"WeaviateClient initialized with port: {self.port} and Ollama endpoint: {self.ollama_api_endpoint}")

    def _connect_client(self):
        """
        Internal method to connect to the Weaviate instance.
        Ensures a connection is established before performing operations.
        """
        try:
            if not self.client or not self.client.is_connected():
                self.client = weaviate.connect_to_local(port=self.port)
                logging.info(f"Successfully connected to Weaviate at port {self.port}.")
            return self.client
        except Exception as e:
            logging.error(f"Failed to connect to Weaviate at port {self.port}: {e}")
            print(f"Error: Failed to connect to Weaviate at port {self.port}. Please ensure Weaviate is running. {e}")
            self.client = None # Reset client if connection fails
            raise # Re-raise to let calling methods handle connection failure

    def _close_client(self):
        """
        Internal method to close the Weaviate client connection.
        """
        if self.client and self.client.is_connected():
            self.client.close()
            logging.info("Weaviate client connection closed.")
        self.client = None # Clear the client instance after closing

    def create_collection(self, name: str):
        """
        Create a collection in the Weaviate instance.

        Args:
            name (str): The name of the collection to create.
        """
        try:
            client = self._connect_client()
            if client:
                logging.info(f"Attempting to create collection: '{name}'")
                print(f"Creating collection: {name}")

                # Check if collection already exists to avoid error
                existing_collections = client.collections.list_all()
                if name in existing_collections:
                    print(f"Collection '{name}' already exists. Skipping creation.")
                    logging.info(f"Collection '{name}' already exists. Skipping creation.")
                    return

                client.collections.create(
                    name=name,
                    vectorizer_config=Configure.Vectorizer.text2vec_ollama(
                        api_endpoint=self.ollama_api_endpoint,
                        model="nomic-embed-text"
                    )
                )
                logging.info(f"Collection '{name}' created successfully.")
                print(f"Collection '{name}' created successfully.")
        except Exception as e:
            logging.error(f"Error creating collection '{name}': {e}")
            print(f"Error creating collection '{name}': {e}")
        finally:
            self._close_client()

    def delete_collection(self, name: str):
        """
        Delete a collection from the Weaviate instance.

        Args:
            name (str): The name of the collection to delete.
        """
        try:
            client = self._connect_client()
            if client:
                logging.info(f"Attempting to delete collection: '{name}'")
                client.collections.delete(name=name)
                logging.info(f"Collection '{name}' deleted successfully.")
                print(f"Collection '{name}' deleted successfully.")
                return True
        except Exception as e:
            logging.error(f"Error deleting collection '{name}': {e}")
            print(f"Error deleting collection '{name}': {e}")
            return "Error:"+str(e)
        finally:
            self._close_client()



    def get_collection_data(self, name: str) -> List[Dict[str, Any]]:
        """
        Retrieve all data objects from a specified collection.

        Args:
            name (str): The name of the collection to retrieve data from.

        Returns:
            List[Dict[str, Any]]: A list of dictionaries, where each dictionary
                                   represents an object's properties in the collection.
        """
        ls = []
        try:
            client = self._connect_client()
            if client:
                logging.info(f"Attempting to get data from collection: '{name}'")
                collection = client.collections.get(name=name)
                for item in collection.iterator():
                    ls.append(item.properties)
                logging.info(f"Successfully retrieved {len(ls)} objects from collection '{name}'.")
                pprint(ls) # Using pprint as in original code
        except Exception as e:
            logging.error(f"Error getting data from collection '{name}': {e}")
            print(f"Error getting data from collection '{name}': {e}")
        finally:
            self._close_client()
        return ls

    def list_collections(self) -> List[weaviate.collections.Collection]:
        """
        List all collections in the Weaviate instance.

        Returns:
            List[weaviate.collections.Collection]: A list of Weaviate Collection objects.
        """
        collections = []
        try:
            client = self._connect_client()
            if client:
                logging.info("Attempting to list all collections.")
                collections = client.collections.list_all()
                logging.info(f"Found {len(collections)} collections.")
                for col in collections:
                    print(f"- {col}")
        except Exception as e:
            logging.error(f"Error getting collections: {e}")
            print(f"Error getting collections: {e}")
        finally:
            self._close_client()
        return collections

    def add_data(self, data: List[Dict[str, Any]], collection_name: str):
        """
        Add data (list of dictionaries) to a specified Weaviate collection.

        Args:
            data (List[Dict[str, Any]]): A list of dictionaries, where each dictionary
                                           represents an object to be added.
            collection_name (str): The name of the collection to add data to.
        """
        try:
            client = self._connect_client()
            if client:
                logging.info(f"Attempting to add {len(data)} objects to collection '{collection_name}'.")
                store = client.collections.get(name=collection_name)

                with store.batch.fixed_size(batch_size=200) as batch:
                    if type(data) != list:
                        data = [data]
                    for d in data:
                        # print(data.index(d)) # Original line, might be heavy for large lists
                        if batch.number_errors > 10:
                            print("Batch import stopped due to excessive errors.")
                            logging.warning("Batch import stopped due to excessive errors.")
                            break
                        # Weaviate `add_object` returns the UUID of the added object.
                        # The original code printed `status` which was this UUID.
                        status = batch.add_object(d)
                        # print("status", status) # Uncomment if you want to see UUIDs during batching

                # Check for errors in the batch import
                failed_objects = store.batch.failed_objects
                if failed_objects:
                    logging.error(f"Number of failed imports for '{collection_name}': {len(failed_objects)}")
                    logging.error(f"First failed object details: {failed_objects[0]}")
                    print(f"Number of failed imports: {len(failed_objects)}")
                    print(f"First failed object: {failed_objects[0]}")
                    self._close_client()
                    return f"Number of failed inserts for '{collection_name}': {len(failed_objects)}"
                else:
                    logging.info(f"Successfully added all {len(data)} objects to collection '{collection_name}'.")
                    print(f"Successfully added all {len(data)} objects to collection '{collection_name}'.")
                    self._close_client()
                    return True

        except Exception as e:
            logging.error(f"Error adding data to collection '{collection_name}': {e}")
            print(f"Error adding data to collection '{collection_name}': {e}")
        finally:
            self._close_client()

    def see_data(self, collection_name: str = "snippets"):
        """
        Iterate and print all objects from a specified collection.
        This is similar to `get_collection_data` but prints directly and doesn't return a list.

        Args:
            collection_name (str): The name of the collection to see data from.
        """
        try:
            client = self._connect_client()
            if client:
                logging.info(f"Attempting to iterate and print data from collection: '{collection_name}'")
                collection = client.collections.get(name=collection_name)
                for item in collection.iterator():
                    print(item)
                logging.info(f"Finished iterating data from collection '{collection_name}'.")
        except Exception as e:
            logging.error(f"Error seeing data in collection '{collection_name}': {e}")
            print(f"Error seeing data in collection '{collection_name}': {e}")
        finally:
            self._close_client()

    def search(self, query: str, k: int = 10, collection_name: str = "snippets") -> Union[List[Dict[str, Any]], str]:
        """
        Perform a semantic search in the Weaviate instance.

        Args:
            query (str): The text query for semantic search.
            k (int): The maximum number of results to return (limit).
            collection_name (str): The name of the collection to search within.

        Returns:
            Union[List[Dict[str, Any]], str]: A list of dictionaries representing the search results,
                                               or a string error message if an error occurs.
        """
        results = []
        try:
            client = self._connect_client()
            if client:
                logging.info(f"Performing search in '{collection_name}' for query: '{query}' (limit: {k})")
                target_collection = client.collections.get(collection_name)

                response = target_collection.query.near_text(
                    query=query,
                    limit=k
                )

                for obj in response.objects:
                    results.append(obj.properties)

                logging.info(f"Found {len(results)} results for query: '{query}'.")
                pprint(results) # Using pprint as in original code

                if not results: # If no results found, return an informative message
                    return "No results found for your query."

                return results

        except Exception as e:
            logging.error(f"Error performing semantic search in '{collection_name}' for query '{query}': {e}")
            print(f"Error performing semantic search: {e}")
            return "Error reaching the LLM or Vector instance, or other search error."
        finally:
            self._close_client()

    def load_data(self, collection_name: str, file_name: str = "docs.json"):
        """
        Load data from a JSON file into a Weaviate collection.
        Note: The original code had a hardcoded list of data `ls` that overwrote
              the data loaded from the file. This version prioritizes file loading.

        Args:
            collection_name (str): The name of the Weaviate collection to load data into.
            file_name (str): The name of the JSON file containing the data.
                             Expected to be in a subdirectory named `data_{collection_name}`.
        """
        data_path = file_name
        data_to_add = []

        try:
            if not os.path.exists(data_path):
                logging.warning(f"Data file not found at: {data_path}. Using hardcoded example data.")
                print(f"Warning: Data file not found at: {data_path}. Using hardcoded example data.")
                # Original hardcoded data as fallback
                data_to_add = [{"story1": "story1_code"}, {"story2": "story2_code"}]
            else:
                with open(data_path, 'r') as f:
                    data_to_add = json.load(f)
                logging.info(f"Loaded {len(data_to_add)} objects from '{data_path}'.")
                pprint(data_to_add)

            # Ensure the collection exists before adding data
            # self.create_collection(collection_name) # This would create it, but add_data handles collection fetching

            self.add_data(data=data_to_add, collection_name=collection_name)

        except json.JSONDecodeError as e:
            logging.error(f"Error decoding JSON from '{data_path}': {e}")
            print(f"Error: Invalid JSON format in '{data_path}': {e}")
        except Exception as e:
            logging.error(f"Error loading data for collection '{collection_name}': {e}")
            print(f"Error loading data for collection '{collection_name}': {e}")


# --- Example Usage (equivalent to the original __main__ block) ---
if __name__ == "__main__":
    from sys import argv
    
    # Initialize the Weaviate client
    # You can customize port and ollama_api_endpoint here if needed
    weaviate_helper = WeaviateClient(port=8080)

    if len(argv) < 2:
        print("Usage: python weaviate_client_module.py <command_code> <collection_name> [data_file_name]")
        print("Commands:")
        print("  0: Create collection")
        print("  1: Get collection data")
        print("  2: List all collections")
        print("  3: Add data from docs.json to collection (requires data_{collection_name}/docs.json)")
        print("  4: Perform search query (default query 'story')")
        print("  5: Load data from specified collection_name and file_name")
        print("  6: Delete collection")
        print("  7: See data (iterate and print all objects from a collection)")
        exit(1)
    
    try:
        command_code = int(argv[1])
        if len(argv)>2:
            collection_name = argv[2]
        else:
            collection_name = ""

        data_file_name = argv[3] if len(argv) > 3 else "docs.json" # Default for command 5

        print(f"\n--- Executing Command: {command_code} for Collection: '{collection_name}' ---")

        if command_code == 0:
            weaviate_helper.create_collection(collection_name)
        elif command_code == 1:
            weaviate_helper.get_collection_data(collection_name)
        elif command_code == 2:
            weaviate_helper.list_collections()
        elif command_code == 3:
            # Assumes data is in a folder like 'data_codegenjjs/docs.json'
            # For this to work, ensure you have a directory like 'data_YOUR_COLLECTION_NAME'
            # and a 'docs.json' file inside it, or modify the load_data method's path.
            # Example: python weaviate_client_module.py 3 codegenjjs
            weaviate_helper.load_data(collection_name, "mydata.json")
        elif command_code == 4:
            search_query = r'story' # Default search query
            if len(argv) > 3:
                search_query = argv[3] # Allow user to specify search query as 4th argument
                print(f"Using custom search query: '{search_query}'")
            results = weaviate_helper.search(search_query, 30, collection_name)
            # print("Search Results:", results) # Already printed by pprint in method
        elif command_code == 5:
            # Example: python weaviate_client_module.py 5 my_corpus my_data.json
            if len(argv) < 4:
                print("Command 5 requires a collection_name and a file_name.")
                print("Usage: python weaviate_client_module.py 5 <collection_name> <file_name>")
            else:
                weaviate_helper.load_data(collection_name, data_file_name)
        elif command_code == 6:
            weaviate_helper.delete_collection(collection_name)
        elif command_code == 7:
            weaviate_helper.see_data(collection_name)
        else:
            print(f"Unknown command code: {command_code}")

    except ValueError:
        print(f"Invalid command code: '{argv[1]}'. Please provide an integer.")
    except Exception as e:
        logging.critical(f"An unhandled error occurred in main execution: {e}", exc_info=True)
        print(f"An unexpected error occurred: {e}")

