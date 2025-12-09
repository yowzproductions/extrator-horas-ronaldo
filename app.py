import streamlit as st
import pandas as pd
from bs4 import BeautifulSoup
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime
import re

# --- 1. CONFIGURAÇÃO DA PÁGINA ---
st.set_page_config(page_title="Processador de Comissões", layout="wide")

st.title("📊 Processador de Comissões em Lote")
st.write("Identifica técnicos, horas vendidas e a data de competência do relatório.")

# --- 2. CONEXÃO SEGURA ---
def conectar_sheets():
    scope = ['https://www.googleapis.com/auth/spreadsheets', 
             'https://www.googleapis.com/auth/drive']
    credentials_dict = st.secrets["gcp_service_account"]
    creds = Credentials.from_service_account_info(credentials_dict, scopes=scope)
    client = gspread.authorize(creds)
    return client

# --- 3. UPLOAD DO ARQUIVO ---
arquivo = st.file_uploader("Solte o relatório HTML aqui", type=["html", "htm"])

if arquivo:
    # Lê o arquivo
    conteudo = arquivo.read().decode("utf-8", errors='ignore')
    soup = BeautifulSoup(conteudo, "html.parser")
    
    # --- CAPTURA INTELIGENTE DA DATA ---
    texto_completo = soup.get_text(separator=" ", strip=True)
    
    # Procura data após a palavra "até"
    match_data = re.search(r"até\s+(\d{2}/\d{2}/\d{4})", texto_completo, re.IGNORECASE)
    
    if match_data:
        data_relatorio = match_data.group(1)
        st.success(f"📅 Data do Relatório identificada: {data_relatorio}")
    else:
        # Tenta pegar qualquer data no início
        match_generico = re.search(r"(\d{2}/\d{2}/\d{4})", texto_completo)
        if match_generico:
            data_relatorio = match_generico.group(1)
            st.warning(f"⚠️ Usei a primeira data encontrada: {data_relatorio}. Confirme se está correta.")
        else:
            data_relatorio = datetime.now().strftime("%d/%m/%Y")
            st.error("⚠️ Não encontrei data. Usando hoje.")

    # --- INÍCIO DO PROCESSAMENTO ---
    dados_para_enviar = []
    tecnico_atual = None
    linhas = soup.find_all("tr")
    
    st.write(f"🔍 Analisando {len(linhas)} linhas do arquivo...")
    
    for linha in linhas:
        texto_linha = linha.get_text(separator=" ", strip=True).upper()
        
        # TRAVA DE SEGURANÇA
        if "TOTAL DA FILIAL" in texto_linha or "TOTAL DA EMPRESA" in texto_linha:
            st.info("Fim da lista de técnicos identificada (Totais gerais ignorados).")
            break
        
        # --- AQUI ESTÁ A CORREÇÃO DA SIGLA ---
        if "TOTAL DO FUNCIONARIO" in texto_linha:
            try:
                # 1. Pega o que vem depois de "TOTAL DO FUNCIONARIO"
                parte_nome = texto_linha.split("TOTAL DO FUNCIONARIO")[1]
                # 2. Remove dois pontos e espaços extras
                texto_sujo = parte_nome.replace(":", "").strip()
                # 3. PEGA APENAS A PRIMEIRA PALAVRA (A Sigla)
                tecnico_atual = texto_sujo.split()[0] 
            except:
                continue 
                
        # Se tem técnico, busca horas
        if tecnico_atual and "HORAS VENDIDAS:" in texto_linha:
            celulas = linha.find_all("td")
            
            for celula in celulas:
                texto_celula = celula.get_text(strip=True).upper()
                
                if "HORAS" in texto_celula and any(c.isdigit() for c in texto_celula) and "VENDIDAS" not in texto_celula:
                    valor_limpo = texto_celula.replace("HORAS", "").strip()
                    
                    dados_para_enviar.append([data_relatorio, arquivo.name, tecnico_atual, valor_limpo])
                    break 

    # --- 4. EXIBIÇÃO E ENVIO ---
    if len(dados_para_enviar) > 0:
        df = pd.DataFrame(dados_para_enviar, columns=["Data Ref.", "Arquivo", "Técnico", "Horas"])
        st.success(f"Encontrei {len(dados_para_enviar)} registros de técnicos!")
        st.dataframe(df)
        
        if st.button("Confirmar e Gravar"):
            with st.spinner("Gravando..."):
                try:
                    client = conectar_sheets()
                    
                    # SEU ID DA PLANILHA
                    ID_PLANILHA = "1XibBlm2x46Dk5bf4JvfrMepD4gITdaOtTALSgaFcwV0"
                    
                    arquivo_sheet = client.open_by_key(ID_PLANILHA)
                    
                    try:
                        aba = arquivo_sheet.worksheet("Comissoes")
                    except:
                        st.error("❌ Erro: Não achei a aba 'Comissoes'.")
                        st.stop()
                    
                    aba.append_rows(dados_para_enviar)
                    
                    st.balloons()
                    st.success(f"✅ Sucesso! Dados de {data_relatorio} gravados.")
                    
                except Exception as e:
                    if "200" in str(e):
                        st.balloons()
                        st.success("✅ Sucesso confirmado (Protocolo 200).")
                    else:
                        st.error(f"Erro: {e}")
    else:
        st.warning("Nenhum dado encontrado.")
