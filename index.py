import streamlit as st

st.set_page_config(page_title="📊 Portal de Análises", layout="wide")
st.title("📌 Portal de Análises")

st.write("Clique abaixo para abrir a análise unificada.")
if st.button("➡️ Abrir Análise Completa"):
    st.switch_page("pages/analise_completo.py")
