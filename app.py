import streamlit as st
import pandas as pd
from bs4 import BeautifulSoup
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime
import re # Biblioteca para expressões regulares (achar padrões de texto)

# --- 1. CONFIGURAÇÃO DA PÁGINA ---
st.set_page_config(page_title="Processador de Comissões", layout="wide")

st.title("📊 Processador de Comissões em Lote")
st.write("Identifica cada técnico e suas respectivas horas vendidas automaticamente.")

# --- 2. CONEXÃO SEGURA ---
def conectar_sheets():
    scope = ['https://www.googleapis.com/auth/spreadsheets', 
             'https://www.googleapis.com/auth/drive']
    credentials_dict = st.secrets["gcp_service_account"]
    creds = Credentials.from_service_account_info(credentials_dict, scopes=scope)
    client = gspread.authorize(creds)
    return client

# --- 3. UPLOAD ---
arquivo = st.file_uploader("Solte o relatório HTML aqui", type=["html", "htm"])

if arquivo:
    conteudo = arquivo.read().decode("utf-8", errors='ignore')
    soup = BeautifulSoup(conteudo, "html.parser")
    
    # Lista para guardar todos os dados encontrados antes de enviar
    dados_para_enviar = []
    
    # Variável para memorizar qual técnico estamos lendo no momento
    tecnico_atual = None
    
    # Estratégia: Pegar todas as linhas da tabela (tr) e ler uma por uma
    linhas = soup.find_all("tr")
    
    st.write(f"🔍 Analisando {len(linhas)} linhas do arquivo...")
    
    for linha in linhas:
        texto_linha = linha.get_text(separator=" ", strip=True).upper()
        
        # 1. Tenta achar a linha que define o funcionário
        if "TOTAL DO FUNCIONARIO" in texto_linha:
            # Exemplo de texto: "TOTAL DO FUNCIONARIO AAD:"
            # Vamos separar por espaço e pegar a sigla
            # Removemos os dois pontos se tiver
            try:
                parte_nome = texto_linha.split("TOTAL DO FUNCIONARIO")[1]
                tecnico_atual = parte_nome.replace(":", "").strip()
            except:
                continue # Se der erro, pula pra próxima linha
                
        # 2. Se já temos um técnico na memória, procuramos as horas
        if tecnico_atual and "HORAS VENDIDAS:" in texto_linha:
            # Achar as células (td) dessa linha específica
            celulas = linha.find_all("td")
            
            # Baseado na estrutura que você mandou, o valor está numa célula à direita
            # Vamos varrer as células procurando a que tem números e "HORAS"
            for celula in celulas:
                texto_celula = celula.get_text(strip=True).upper()
                
                # Verifica se parece um valor de hora (tem número e 'HORAS')
                # Ignora a célula que diz só "HORAS VENDIDAS:"
                if "HORAS" in texto_celula and any(c.isdigit() for c in texto_celula) and "VENDIDAS" not in texto_celula:
                    valor_limpo = texto_celula.replace("HORAS", "").strip()
                    
                    # Adiciona na nossa lista final
                    timestamp = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
                    dados_para_enviar.append([timestamp, arquivo.name, tecnico_atual, valor_limpo])
                    
                    # Importante: Não limpamos o tecnico_atual aqui, 
                    # pois podem haver outras informações dele abaixo.
                    # Ele só muda quando o loop achar outro "TOTAL DO FUNCIONARIO"
                    break 

    # --- 4. EXIBIÇÃO E CONFIRMAÇÃO ---
    if len(dados_para_enviar) > 0:
        df = pd.DataFrame(dados_para_enviar, columns=["Data", "Arquivo", "Técnico", "Horas"])
        st.success(f"Encontrei {len(dados_para_enviar)} registros!")
        st.dataframe(df) # Mostra uma tabela prévia na tela
        
        if st.button("Confirmar e Gravar TUDO no Sheets"):
            with st.spinner("Enviando dados em lote..."):
                try:
                    client = conectar_sheets()
                    sheet = client.open("Dados_HTML").sheet1 
                    
                    # append_rows (no plural) é mais rápido para muitos dados
                    sheet.append_rows(dados_para_enviar)
                    
                    st.balloons()
                    st.success("✅ Todos os técnicos foram salvos na planilha!")
                except Exception as e:
                    st.error(f"Erro ao salvar: {e}")
    else:
        st.warning("Não consegui identificar nenhum padrão 'TOTAL DO FUNCIONARIO' seguido de 'HORAS VENDIDAS'. Verifique o arquivo.")
