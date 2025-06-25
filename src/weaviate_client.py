import streamlit as st
import weaviate_wrapper as weaviate_wrapper
import pandas as pd
import json
import os

stst=st.session_state

st.markdown(f'<H6>Weaviate client</H6>',unsafe_allow_html=True)


class WeaviateStreamlitApp:
    def __init__(self, port: int = 8080):
        # Initialize Weaviate client and session state
        self.wv = weaviate_wrapper.WeaviateClient(port=port)
        self.stst = st.session_state
        self.modes=["Fetch rows","Insert data","Search","New collection","Bulk insert","Delete collection"]
        self.initialize_collection()
        self.pivot()
        

        

    def initialize_collection(self):
        try:
            collections_dict = self.wv.list_collections()
            # Convert dictionary keys to a list for selectbox
            collections_list = list(collections_dict.keys()) if collections_dict else []
            
            if not collections_list:
                st.warning("No collections found. Please create one first.")
                self.stst["collection"] = None
                self.create_collection()
                return

            self.stst["collections"] = collections_list
            collection = st.sidebar.selectbox("Collection", self.stst["collections"])
            self.stst["collection"] = collection
        except Exception as e:
            st.error(f"Failed to initialize collection. Check connection and port. : \n {str(e)}")

    def fetch_rows(self, numrows: int = 20):
        """
        Fetches and displays rows from the selected Weaviate collection.

        Args:
            numrows (int): The number of rows to display in the data editor.
        """
        if not self.stst.get("collection"):
            st.info("Please select or create a collection first.")
            return
        ls = self.wv.get_collection_data(self.stst['collection'])
        if ls:
            data = pd.json_normalize(ls)
            st.data_editor(data, use_container_width=True, num_rows=numrows, hide_index=True)
        else:
            st.info(f"No data found in collection '{self.stst['collection']}'.")

    def search(self):
        if not self.stst.get("collection"):
            st.info("Please select or create a collection first.")
            return
        txt = st.text_input("Search Query")
        srch = st.button("Search")
        if srch and txt:
            res = self.wv.search(txt, collection_name=self.stst['collection'])
            if isinstance(res, list) and res:
                st.dataframe(pd.json_normalize(res))
            else:
                st.info(res)
    def create_collection(self):
        collection_name=st.text_input("Collection name")
        if st.button("Create"):
            try:
                self.wv.create_collection(collection_name)
            except:
                st.error("Error creating collection")
            else:
                st.success(f"The collection {collection_name} created successfully")
    

    def insert_data(self):

        if not self.stst.get("collection"):
            st.info("Please select or create a collection first.")
            return

        s=st.expander("Put the data in JSON",expanded=True)
        data=s.text_area("Data in JSON")
        if s.button("Insert"):
            try:
                dic=json.loads(data)
            except Exception as e:
                st.error("Invalid JSON format")
                return
            try:
                res=self.wv.add_data(data=dic, collection_name=stst['collection'])
                if res ==True:
                    st.success("Data inserted successfully")
                else:
                    st.error("Error inserting data:\n"+res)
            except Exception as e:
                st.error(f"Error inserting data: {e}")

    def insert_bulk_data(self):

        if not self.stst.get("collection"):
            st.info("Please select or create a collection first.")
            return
        if not os.path.exists("temp"):
            os.mkdir("temp")
        with st.form("Insert bulk data", clear_on_submit=True):
            fl = st.file_uploader("Upload JSON file")
            submitted = st.form_submit_button("upload")
            if submitted ==True and fl is not None:
                with open(f"temp/{fl.name}","wb") as f:
                    f.write(fl.getbuffer())
                    stst["fname"]=fl.name
            dic=None
            try:
                if stst["fname"]:
                    with open(f"temp/{stst['fname']}","r") as f:
                        dic=json.load(f)                                        
            except Exception as e:
                st.error("Invalid JSON format? :"+str(e))
                return
        if st.button("Insert"):
            try:
                if dic:
                    res=self.wv.add_data(data=dic, collection_name=stst['collection'])
                if res ==True:
                    st.success("Data inserted successfully")
                    os.remove(f"temp/{stst['fname']}")
                    stst["fname"]=None
                else:
                    st.error("Error inserting data:\n"+res)
            except Exception as e:
                st.error(f"Error inserting data: {e}")


    def delete_collection(self):
            s=st.sidebar.empty()
            sx=s.expander("Delete collection",expanded=True)
            
            sx.warning(f"Are you sure you want to delete the collection: {stst['collection']}?")
            if sx.button(f"Yes, go ahead"):

                try:
                    res=self.wv.delete_collection(stst['collection'])
                    if res==True:
                        s.success(f"Collection '{stst['collection']}' deleted successfully.")

                except Exception as e:
                    s.error(f"Error deleting collection '{stst['collection']}': {e}")



    def pivot(self):
        """
        Pivots the application's functionality based on the selected mode in the sidebar.
        Each mode (Fetch rows, Insert data, Search, New collection) calls the corresponding
        method of the WeaviateStreamlitApp class, wrapped in a try-except block for error handling.
        """
        modee=st.sidebar.selectbox("Mode",self.modes)

        if modee=="Fetch rows":
            try:
                self.fetch_rows()
            except Exception as e:
                st.error(f"Failed to fetch rows. Check connection and port. : \n {str(e)}")
        elif modee=="Insert data":
            try:
                self.insert_data()
            except Exception as e:
                st.error(f"Failed to insert data. Check connection and port. : \n {str(e)}")
        elif modee=="Search":
            try:
                self.search()
            except Exception as e:
                st.error(f"Failed to search. Check connection and port. : \n {str(e)}")
        elif modee=="New collection":
            try:
                self.create_collection()
            except Exception as e:
                st.error(f"Failed to create collection. Check connection and port. : \n {str(e)}")

        elif modee=="Delete collection":
            try:
                self.delete_collection()
            except Exception as e:
                st.error(f"Failed to delete collection. Check connection and port. : \n {str(e)}")
        elif modee=="Bulk insert":
            try:
                self.insert_bulk_data()
            except Exception as e:
                st.error(f"Failed to insert bulk data. Check connection and port. : \n {str(e)}")


                         


    
    



if __name__ == "__main__":
    wsc=WeaviateStreamlitApp(8080)
    
