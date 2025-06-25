# weaviate_local_gui
A GUI to local Weaviate with text embedding support. It has both GUI and API to operate on Local weaviate container with text emdeddings created on nomic-embed-text


# Weaviate Wrapper
Shivaji Basu
github.com/shivajigit
shivajigeek@gmail.com
last updated 22 Jan 2025

# Weaviate Wrapper

This repository contains a Python api wrapper for Weaviate, simplifying interactions with the Weaviate vector database.
It also bootstraps a local llm nomic-embed-text create text-embedding and save in weaviate
The embeddings are automatically created when data is inserted in a weaviate collection
It futher has a GUI for operations on a browser.
The GUI allows below operations :
    -Create a collection
    -Delete a collection
    -Insert data
    -Insert bulk data
    -Search a collection using similarity search
    -Fetch ordered rows from a collection

## Installation

To use the `weaviate_wrapper.py`, you need to set up your environment and potentially a Dockerized Weaviate instance.

### Prerequisites

*   Python 3.11+
*   Docker (for running Weaviate locally and Ollama with Nomic) text embedding

### Steps

1.  **Clone the repository:**

    ```bash
    git clone https://github.com/your-username/weaviate-wrapper.git
    cd weaviate-wrapper
    ```

2.  **Install Python dependencies:**

    It's recommended to use a virtual environment:

    ```bash
    python -m venv venv
    source venv/bin/activate  # On Windows: `venv\Scripts\activate`
    pip install -r requirements.txt
    ```
    (Make sure you have a `requirements.txt` file with `weaviate-client` and any other necessary libraries listed.)

3.  **Set up Weaviate and ollama (using Docker):**

    Bootstrap the docker containers from the compose file: weaviate-ollama-compose
    Run both the containers
    In Ollama container, take the terminal as run:
        ollama run nomic-embed-text
        #this will install the embedding LLM

4. Start GUI running the bash script app.sh
    open the app on localhost:8501

        
