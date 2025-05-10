import streamlit as st
import pandas as pd
from pymongo import MongoClient
from transformers import pipeline
from langchain.llms import HuggingFacePipeline
from langchain.prompts import PromptTemplate
from langchain.chains import LLMChain
import os
import json
from datetime import datetime

# === MongoDB Config ===
MONGO_URI = "mongodb://localhost:27017"
DB_NAME = "gen_ai_db"
COLLECTION_NAME = "products"

# === Set up local LLM (using small model for demo) ===
@st.cache_resource
def setup_local_llm():
    pipe = pipeline("text-generation", model="tiiuae/falcon-rw-1b", max_new_tokens=150)
    llm = HuggingFacePipeline(pipeline=pipe)
    return llm

# === Load CSV to MongoDB ===
def load_csv_to_mongodb(csv_file):
    df = pd.read_csv(csv_file)
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    collection = db[COLLECTION_NAME]
    collection.delete_many({})
    collection.insert_many(df.to_dict(orient="records"))
    return df.columns.tolist()

# === Generate Query using LLM ===
def generate_query(llm, user_question):
    template = """
You are a MongoDB expert. Convert the following question into a valid MongoDB query using the 'products' collection.
Question: {question}
MongoDB Query:
"""
    prompt = PromptTemplate(input_variables=["question"], template=template)
    chain = LLMChain(llm=llm, prompt=prompt)
    return chain.run(user_question).strip()

# === Execute MongoDB Query ===
def execute_query(query_string):
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    collection = db[COLLECTION_NAME]
    try:
        query_dict = eval(query_string.split("find(")[1].rstrip(")"))
        results = list(collection.find(query_dict))
        return results
    except Exception as e:
        st.error(f"Query execution failed: {e}")
        return []

# === Save result CSV ===
def save_result_csv(data):
    if not data:
        return None
    df = pd.DataFrame(data)
    filename = f"result_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    df.to_csv(filename, index=False)
    return filename

# === UI ===
st.title("🧠 Gen-AI Data Query System with MongoDB + LLM")

uploaded_file = st.file_uploader("Upload a CSV file", type="csv")

if uploaded_file:
    columns = load_csv_to_mongodb(uploaded_file)
    st.success("CSV loaded to MongoDB!")

    question = st.text_area("🔍 Ask a question about your data:", height=100)

    if st.button("Generate & Run Query"):
        with st.spinner("Loading local LLM..."):
            llm = setup_local_llm()

        with st.spinner("Generating MongoDB query..."):
            query = generate_query(llm, question)
            st.code(query, language="json")

            # Save query
            with open("Queries_generated.txt", "a") as f:
                f.write(f"Question: {question}\nQuery: {query}\n\n")

        with st.spinner("Running query and retrieving results..."):
            result = execute_query(query)

        if result:
            st.success("✅ Query executed. Preview below:")
            df = pd.DataFrame(result).drop(columns=['_id'], errors='ignore')
            st.dataframe(df)

            filename = save_result_csv(result)
            if filename:
                with open(filename, "rb") as f:
                    st.download_button("⬇️ Download CSV", f, file_name=filename)
        else:
            st.warning("No results found or query failed.")
