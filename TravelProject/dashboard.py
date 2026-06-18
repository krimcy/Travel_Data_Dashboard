import streamlit as st
import pandas as pd

st.title("📊 Data Quality Dashboard")

file = st.file_uploader("Upload CSV")

if file:
    df = pd.read_csv(file)

    st.subheader("Preview")
    st.dataframe(df.head())

    st.subheader("Basic Info")
    st.write(df.describe(include="all"))

    st.subheader("Missing Values")
    st.write(df.isnull().sum())

    st.subheader("Duplicate Rows")
    st.write(df.duplicated().sum())

    st.subheader("Shape")
    st.write(df.shape)