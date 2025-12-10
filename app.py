import streamlit as st
import pandas as pd
from bs4 import BeautifulSoup
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime
import re
import unicodedata

# --- FUNÇÃO EXTRA: REMOVER ACENTOS ---
def remover_acentos(texto):
    return ''.join(c for c in unicodedata.normalize('NFD', texto) if unicodedata.category(c) != 'Mn')

# --- 1. CONFIGURAÇÃO DA PÁGINA ---
st.set_page_config(page_title="Central de Relatórios WLM", layout="wide")
st.title("🏭 Central de Processamento de Relatórios")

# --- 2. CONEXÃO SEGURA ---
def conectar_sheets():
    scope = ['https://www.googleapis.com/auth/spreadsheets', 
             'https://www.googleapis.com/auth/drive']
    credentials_dict = st.secrets["gcp_service_account"]
    creds = Credentials.from_service_account_info(credentials_dict, scopes=scope)
    client = gspread.authorize(creds)
    return client

# ID DA SUA PLANILHA
ID_PLANILHA_MESTRA = "1XibBlm2x46Dk5bf4JvfrMepD4gITdaOtTALSgaFcwV0"

# --- ABAS ---
aba_comissoes, aba_aproveitamento = st.tabs(["💰 Pagamento de Comissões", "⚙️ Aproveitamento Técnico"])

# ==============================================================================
# SISTEMA 1: PAGAMENTO DE COMISSÕES (MANTIDO)
# ==============================================================================
with aba_comissoes:
    st.header("Processador de Comissões")
    st.write("Arraste os relatórios de 'Pagamento de Comissões' (HTML).")
    
    arquivos_comissao = st.file_uploader("Upload Comissões HTML", type=["html", "htm"], accept_multiple_files=True, key="uploader_comissao")

    if arquivos_comissao:
        dados_comissao = []
        st.write(f"📂 Processando {len(arquivos_comissao)} arquivos...")
        
        for arquivo in arquivos_comissao:
            try:
                # Tenta ler como UTF-8, se falhar tenta Latin-1
                try:
                    conteudo = arquivo.read().decode("utf-8")
                except:
                    arquivo.seek(0)
                    conteudo = arquivo.read().decode("latin-1")

                soup = BeautifulSoup(conteudo, "html.parser")
                
                texto_completo = soup.get_text(separator=" ", strip=True)
                match_data = re.search(r"até\s+(\d{2}/\d{2}/\d{4})", texto_completo, re.IGNORECASE)
                data_relatorio = match_data.group(1) if match_data else datetime.now().strftime("%d/%m/%Y")

                tecnico_atual = None
                linhas = soup.find_all("tr")
                
                for linha in linhas:
                    texto_linha = linha.get_text(separator=" ", strip=True).upper()
                    
                    if "TOTAL DA FILIAL" in texto_linha or "TOTAL DA EMPRESA" in texto_linha:
                        break
                    
                    if "TOTAL DO FUNCIONARIO" in texto_linha:
                        try:
                            tecnico_atual = texto_linha.split("TOTAL DO FUNCIONARIO")[1].replace(":", "").strip().split()[0]
                        except:
                            continue 
                            
                    if tecnico_atual and "HORAS VENDIDAS:" in texto_linha:
                        celulas = linha.find_all("td")
                        for celula in celulas:
                            texto_celula = celula.get_text(strip=True).upper()
                            if "HORAS" in texto_celula and any(c.isdigit() for c in texto_celula) and "VENDIDAS" not in texto_celula:
                                valor_limpo = texto_celula.replace("HORAS", "").strip()
                                dados_comissao.append([data_relatorio, arquivo.name, tecnico_atual, valor_limpo])
                                break 
            except Exception as e:
                st.error(f"Erro no arquivo {arquivo.name}: {e}")

        if len(dados_comissao) > 0:
            df_comissao = pd.DataFrame(dados_comissao, columns=["Data Ref.", "Arquivo", "Técnico", "Horas"])
            st.dataframe(df_comissao)
            
            if st.button("Gravar Comissões no Sheets", key="btn_comissao"):
                with st.spinner("Enviando..."):
                    try:
                        client = conectar_sheets()
                        sheet = client.open_by_key(ID_PLANILHA_MESTRA)
                        aba = sheet.worksheet("Comissoes")
                        aba.append_rows(dados_comissao)
                        st.success(f"✅ Sucesso! {len(dados_comissao)} linhas gravadas.")
                    except Exception as e:
                        if "200" in str(e): st.success("✅ Sucesso (200).")
                        else: st.error(f"Erro: {e}")
        else:
            st.warning("Processamento finalizado, mas nenhum dado foi encontrado. Verifique o arquivo.")

# ==============================================================================
# SISTEMA 2: APROVEITAMENTO TÉCNICO (CORRIGIDO E BLINDADO)
# ==============================================================================
with aba_aproveitamento:
    st.header("Extrator de Aproveitamento (T.Disp / TP / TG)")
    st.write("Arraste os relatórios de 'Aproveitamento Tempo Mecânico' (HTML).")
    
    arquivos_aprov = st.file_uploader("Upload Aproveitamento HTML", type=["html", "htm"], accept_multiple_files=True, key="uploader_aprov")
    
    if arquivos_aprov:
        dados_aprov = []
        st.info(f"📂 Iniciando leitura de {len(arquivos_aprov)} arquivos...")
        
        for arquivo in arquivos_aprov:
            try:
                # --- CORREÇÃO 1: DETECÇÃO DE CODIFICAÇÃO ---
                # O Python tenta ler como UTF-8. Se der erro, tenta Latin-1 (padrão antigo)
                raw_data = arquivo.read()
                try:
                    conteudo = raw_data.decode("utf-8")
                except UnicodeDecodeError:
                    conteudo = raw_data.decode("latin-1")
                
                soup = BeautifulSoup(conteudo, "html.parser")
                tecnico_atual_aprov = None
                linhas = soup.find_all("tr")
                
                for linha in linhas:
                    # Pega o texto e remove acentos para facilitar a comparação
                    texto_original = linha.get_text(separator=" ", strip=True).upper()
                    texto_limpo = remover_acentos(texto_original) # Transforma MECÂNICO em MECANICO
                    
                    # Trava de Segurança
                    if "TOTAL FILIAL:" in texto_original:
                        break

                    # --- CORREÇÃO 2: BUSCA ROBUSTA DO TÉCNICO ---
                    # Procura por "MECANICO" (sem acento) ou "MECANICO:"
                    if "MECANICO" in texto_limpo and ":" in texto_limpo:
                        try:
                            # Divide e pega a parte direita
                            if "MECANICO:" in texto_limpo:
                                parte_direita = texto_limpo.split("MECANICO:")[1]
                            else:
                                # Caso esteja escrito diferente, tenta pegar o final
                                parte_direita = texto_limpo.split("MECANICO")[1]

                            # Lógica de Limpeza da Sigla
                            if "-" in parte_direita:
                                tecnico_temp = parte_direita.split("-")[0].strip()
                            else:
                                tecnico_temp = parte_direita.strip().split()[0]
                            
                            # Remove pontuação extra se tiver (ex: ":AAD")
                            tecnico_atual_aprov = tecnico_temp.replace(":", "")
                            
                        except:
                            continue

                    if "TOT.MEC.:" in texto_original:
                        tecnico_atual_aprov = None
                        continue

                    # 3. Identifica e LIMPA a Data
                    if tecnico_atual_aprov:
                        celulas = linha.find_all("td")
                        if not celulas: continue
                        
                        texto_primeira_celula = celulas[0].get_text(strip=True)
                        
                        # Verifica formato de data DD/MM/YY
                        if re.match(r"\d{2}/\d{2}/\d{2}", texto_primeira_celula):
                            try:
                                data_limpa = texto_primeira_celula.split()[0] # Remove dia da semana
                                
                                # Captura segura das colunas (evita erro se a linha for curta)
                                if len(celulas) >= 4:
                                    t_disp = celulas[1].get_text(strip=True)
                                    tp = celulas[2].get_text(strip=True)
                                    tg = celulas[3].get_text(strip=True)
                                    
                                    dados_aprov.append([data_limpa, arquivo.name, tecnico_atual_aprov, t_disp, tp, tg])
                            except IndexError:
                                continue

            except Exception as e:
                st.error(f"Erro ao ler arquivo {arquivo.name}: {e}")
                
        # --- CORREÇÃO 3: FEEDBACK VISUAL ---
        if len(dados_aprov) > 0:
            df_aprov = pd.DataFrame(dados_aprov, columns=["Data", "Arquivo", "Técnico", "T. Disp", "TP", "TG"])
            st.success(f"✅ Processamento concluído! {len(dados_aprov)} registros encontrados.")
            st.dataframe(df_aprov)
            
            if st.button("Gravar Aproveitamento no Sheets", key="btn_aprov"):
                with st.spinner("Enviando..."):
                    try:
                        client = conectar_sheets()
                        sheet = client.open_by_key(ID_PLANILHA_MESTRA)
                        
                        try:
                            aba = sheet.worksheet("Aproveitamento")
                        except:
                            st.error("❌ Erro: Aba 'Aproveitamento' não encontrada.")
                            st.stop()
                            
                        aba.append_rows(dados_aprov)
                        st.success(f"✅ Sucesso! Dados gravados na aba 'Aproveitamento'.")
                    except Exception as e:
                        if "200" in str(e): st.success("✅ Sucesso (200).")
                        else: st.error(f"Erro: {e}")
        else:
            st.warning("⚠️ O robô leu o arquivo mas não encontrou dados. Possíveis causas: \n1. O layout do HTML mudou. \n2. O arquivo está vazio.")
