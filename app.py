import streamlit as st
import pandas as pd
from bs4 import BeautifulSoup
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime
import re
import unicodedata

# --- CONFIGURAÇÃO DA PÁGINA ---
st.set_page_config(page_title="Central de Relatórios WLM", layout="wide", page_icon="🔒")

# ID da sua planilha
ID_PLANILHA_MESTRA = "1XibBlm2x46Dk5bf4JvfrMepD4gITdaOtTALSgaFcwV0"

# --- FUNÇÕES TÉCNICAS E AUXILIARES ---
def remover_acentos(texto):
    return ''.join(c for c in unicodedata.normalize('NFD', texto) if unicodedata.category(c) != 'Mn')

def conectar_sheets():
    scope = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
    credentials_dict = st.secrets["gcp_service_account"]
    creds = Credentials.from_service_account_info(credentials_dict, scopes=scope)
    client = gspread.authorize(creds)
    return client

def converter_br_para_float(valor):
    if pd.isna(valor) or valor == "": return 0.0
    if isinstance(valor, (int, float)): return float(valor)
    valor_str = str(valor).strip()
    if '.' in valor_str and ',' in valor_str: valor_str = valor_str.replace('.', '')
    valor_str = valor_str.replace(',', '.')
    try: return float(valor_str)
    except: return 0.0

def verificar_acesso():
    try:
        client = conectar_sheets()
        sh = client.open_by_key(ID_PLANILHA_MESTRA)
        try: return sh.worksheet("Config").acell('B1').value
        except: return 'admin'
    except: return None

# --- LÓGICA DE PARSEAMENTO HTML (EXTRAÇÃO) ---
# Separamos isso para poder chamar de qualquer lugar
def parse_comissoes(arquivos):
    dados = []
    for arquivo in arquivos:
        try:
            try: conteudo = arquivo.read().decode("utf-8")
            except: 
                arquivo.seek(0)
                conteudo = arquivo.read().decode("latin-1")
            # Reseta o ponteiro do arquivo para caso precise ler de novo
            arquivo.seek(0)
            
            soup = BeautifulSoup(conteudo, "html.parser")
            texto_completo = soup.get_text(separator=" ", strip=True)
            match_data = re.search(r"até\s+(\d{2}/\d{2}/\d{4})", texto_completo, re.IGNORECASE)
            data_relatorio = match_data.group(1) if match_data else datetime.now().strftime("%d/%m/%Y")
            tecnico_atual = None
            for linha in soup.find_all("tr"):
                texto_linha = linha.get_text(separator=" ", strip=True).upper()
                if "TOTAL DA FILIAL" in texto_linha or "TOTAL DA EMPRESA" in texto_linha: break
                if "TOTAL DO FUNCIONARIO" in texto_linha:
                    try: tecnico_atual = texto_linha.split("TOTAL DO FUNCIONARIO")[1].replace(":", "").strip().split()[0]
                    except: continue 
                if tecnico_atual and "HORAS VENDIDAS:" in texto_linha:
                    celulas = linha.find_all("td")
                    for celula in celulas:
                        txt = celula.get_text(strip=True).upper()
                        if "HORAS" in txt and any(c.isdigit() for c in txt) and "VENDIDAS" not in txt:
                            dados.append([data_relatorio, arquivo.name, tecnico_atual, txt.replace("HORAS", "").strip()])
                            break 
        except Exception as e: st.error(f"Erro no arquivo {arquivo.name}: {e}")
    return dados

def parse_aproveitamento(arquivos):
    dados = []
    for arquivo in arquivos:
        try:
            raw_data = arquivo.read()
            # Reseta ponteiro
            arquivo.seek(0)
            try: conteudo = raw_data.decode("utf-8")
            except:
                try: conteudo = raw_data.decode("latin-1")
                except: conteudo = raw_data.decode("utf-16")
            
            soup = BeautifulSoup(conteudo, "html.parser")
            tecnico_atual_aprov = None
            linhas = soup.find_all("tr")
            for linha in linhas:
                texto_original = linha.get_text(separator=" ", strip=True).upper()
                texto_limpo = remover_acentos(texto_original)
                if "TOTAL FILIAL:" in texto_original: break
                if "MECANICO" in texto_limpo and "TOT.MEC" not in texto_limpo:
                    try:
                        parte_direita = texto_limpo.split("MECANICO")[1].replace(":", "").strip()
                        if "-" in parte_direita: tecnico_atual_aprov = parte_direita.split("-")[0].strip()
                        else: tecnico_atual_aprov = parte_direita.split()[0]
                    except: continue
                if "TOT.MEC.:" in texto_original: tecnico_atual_aprov = None; continue
                if tecnico_atual_aprov:
                    celulas = linha.find_all("td")
                    if not celulas: continue
                    txt_cel0 = celulas[0].get_text(strip=True)
                    if re.match(r"\d{2}/\d{2}/\d{2}", txt_cel0):
                        try:
                            if len(celulas) >= 4:
                                dados.append([txt_cel0.split()[0], arquivo.name, tecnico_atual_aprov, 
                                              celulas[1].get_text(strip=True), celulas[2].get_text(strip=True), celulas[3].get_text(strip=True)])
                        except: continue
        except Exception as e: st.error(f"Erro no arquivo {arquivo.name}: {e}")
    return dados

# --- FUNÇÕES DE BANCO DE DADOS (GSPREAD) ---

def salvar_com_upsert(nome_aba, novos_dados_df, colunas_chaves):
    """Lê, Mescla, Remove Duplicatas e Salva."""
    client = conectar_sheets()
    sh = client.open_by_key(ID_PLANILHA_MESTRA)
    
    try:
        ws = sh.worksheet(nome_aba)
        dados_antigos = ws.get_all_records()
        df_antigo = pd.DataFrame(dados_antigos)
    except:
        ws = sh.add_worksheet(title=nome_aba, rows=1000, cols=20)
        df_antigo = pd.DataFrame()

    # Converter tudo para string para comparação segura
    if not df_antigo.empty:
        for col in df_antigo.columns: df_antigo[col] = df_antigo[col].astype(str)
    for col in novos_dados_df.columns: novos_dados_df[col] = novos_dados_df[col].astype(str)

    # Upsert
    df_total = pd.concat([df_antigo, novos_dados_df])
    df_final = df_total.drop_duplicates(subset=colunas_chaves, keep='last')

    # Limpeza e Gravação
    ws.clear()
    ws.update('A1', [df_final.columns.values.tolist()] + df_final.values.tolist())
    return len(df_final)

def processar_unificacao():
    """Lê as abas salvas, limpa tipos e grava no Consolidado."""
    try:
        client = conectar_sheets()
        sh = client.open_by_key(ID_PLANILHA_MESTRA)
        ws_com = sh.worksheet("Comissoes")
        ws_aprov = sh.worksheet("Aproveitamento")

        dados_com = ws_com.get_all_records()
        dados_aprov = ws_aprov.get_all_records()

        if not dados_com or not dados_aprov: return False

        df_com = pd.DataFrame(dados_com)
        df_aprov = pd.DataFrame(dados_aprov)

        # Padronização de Colunas
        df_com.columns = [c.strip() for c in df_com.columns]
        df_aprov.columns = [c.strip() for c in df_aprov.columns]
        
        renomear_comissao = {"Data Processamento": "Data", "Sigla Técnico": "Técnico"}
        df_com.rename(columns=renomear_comissao, inplace=True)

        # Seleção
        cols_com = ['Data', 'Técnico', 'Horas Vendidas']
        df_com = df_com[[c for c in cols_com if c in df_com.columns]]
        
        cols_aprov = ['Data', 'Técnico', 'Disp', 'TP', 'TG']
        df_aprov = df_aprov[[c for c in cols_aprov if c in df_aprov.columns]]

        # TRATAMENTO NUMÉRICO (IMPORTANTE)
        for col in ['Horas Vendidas', 'Disp', 'TP', 'TG']:
            if col in df_com.columns: df_com[col] = df_com[col].apply(converter_br_para_float)
            if col in df_aprov.columns: df_aprov[col] = df_aprov[col].apply(converter_br_para_float)

        # Chaves como String para Merge
        df_com['Key_D'] = df_com['Data'].astype(str)
        df_com['Key_T'] = df_com['Técnico'].astype(str)
        df_aprov['Key_D'] = df_aprov['Data'].astype(str)
        df_aprov['Key_T'] = df_aprov['Técnico'].astype(str)

        # Merge
        df_final = pd.merge(
            df_com, df_aprov, 
            left_on=['Key_D', 'Key_T'], right_on=['Key_D', 'Key_T'], 
            how='outer', suffixes=('_C', '_A')
        )
        
        # Consolida Data e Técnico e Preenche Zeros
        df_final.fillna(0, inplace=True)
        
        # Resolve conflito de nomes de colunas
        df_final['Data'] = df_final.apply(lambda x: x['Data_C'] if x['Data_C'] != 0 and str(x['Data_C']) != "0" else x['Data_A'], axis=1)
        df_final['Técnico'] = df_final.apply(lambda x: x['Técnico_C'] if x['Técnico_C'] != 0 and str(x['Técnico_C']) != "0" else x['Técnico_A'], axis=1)

        # Seleciona Finais
        cols_finais = ['Data', 'Técnico', 'Horas Vendidas', 'Disp', 'TP', 'TG']
        df_final = df_final[[c for c in cols_finais if c in df_final.columns]]

        # --- A CORREÇÃO DE PREENCHIMENTO ---
        # Converte tudo para tipos nativos do Python para o Gspread não reclamar (numpy killers)
        df_final = df_final.astype(object) 
        df_final.fillna("", inplace=True) # JSON não aceita NaN
        
        try: ws_final = sh.worksheet("Consolidado")
        except: ws_final = sh.add_worksheet(title="Consolidado", rows=2000, cols=20)
        
        ws_final.clear()
        ws_final.update('A1', [df_final.columns.values.tolist()] + df_final.values.tolist())
        return True
    except Exception as e:
        print(f"Erro unificação: {e}")
        return False

# --- ROTINA MESTRA DE GRAVAÇÃO (GLOBAL) ---
def executar_rotina_global(df_com=None, df_aprov=None):
    """Salva TUDO o que estiver disponível e atualiza o consolidado."""
    status_msg = st.empty()
    bar = st.progress(0)
    
    try:
        # 1. Salva Comissões se houver dados
        if df_com is not None and not df_com.empty:
            status_msg.info("💾 Salvando Comissões...")
            salvar_com_upsert("Comissoes", df_com, ["Data Processamento", "Sigla Técnico"])
            bar.progress(40)
        
        # 2. Salva Aproveitamento se houver dados
        if df_aprov is not None and not df_aprov.empty:
            status_msg.info("💾 Salvando Aproveitamento...")
            salvar_com_upsert("Aproveitamento", df_aprov, ["Data", "Técnico"])
            bar.progress(70)
            
        # 3. Sempre tenta unificar
        status_msg.info("🔄 Atualizando Relatório Consolidado...")
        sucesso = processar_unificacao()
        bar.progress(100)
        
        if sucesso:
            status_msg.success("✅ Processo Completo! Todas as bases foram atualizadas.")
            st.balloons()
        else:
            status_msg.warning("⚠️ Bases salvas, mas houve falha na unificação.")
            
    except Exception as e:
        status_msg.error(f"Erro Crítico: {e}")

# ============================================
# INTERFACE PRINCIPAL
# ============================================

st.sidebar.title("Login Seguro")
senha = st.sidebar.text_input("Senha:", type="password")

if senha == verificar_acesso():
    st.sidebar.success("Acesso Liberado")
    st.title("🏭 Central de Processamento WLM")
    
    # Uploaders ficam fora das tabs para garantir acesso global? 
    # Não, mantemos nas tabs por organização, mas checaremos o session state
    
    aba1, aba2 = st.tabs(["💰 Comissões", "⚙️ Aproveitamento"])

    # Variáveis globais para armazenar os dados processados
    df_comissao_global = None
    df_aprov_global = None

    # --- ABA 1 ---
    with aba1:
        st.header("Upload Comissões")
        files_com = st.file_uploader("Arquivos HTML", accept_multiple_files=True, key="up_com")
        if files_com:
            dados_c = parse_comissoes(files_com)
            if dados_c:
                df_comissao_global = pd.DataFrame(dados_c, columns=["Data Processamento", "Nome do Arquivo", "Sigla Técnico", "Horas Vendidas"])
                st.dataframe(df_comissao_global, height=200)

    # --- ABA 2 ---
    with aba2:
        st.header("Upload Aproveitamento")
        files_aprov = st.file_uploader("Arquivos HTML/SLK", accept_multiple_files=True, key="up_aprov")
        if files_aprov:
            dados_a = parse_aproveitamento(files_aprov)
            if dados_a:
                df_aprov_global = pd.DataFrame(dados_a, columns=["Data", "Arquivo", "Técnico", "Disp", "TP", "TG"])
                st.dataframe(df_aprov_global, height=200)

    # --- BOTÃO DE AÇÃO GLOBAL (Visível em ambas as abas ou fixo) ---
    st.divider()
    col_btn, col_txt = st.columns([1, 4])
    
    with col_btn:
        # Este botão agora olha para TUDO
        if st.button("🚀 GRAVAR TUDO E ATUALIZAR", type="primary"):
            if df_comissao_global is None and df_aprov_global is None:
                st.warning("Nenhum arquivo carregado em nenhuma das abas.")
            else:
                executar_rotina_global(df_comissao_global, df_aprov_global)
    
    with col_txt:
        st.caption("ℹ️ Este botão salva os arquivos de Comissões E Aproveitamento simultaneamente, se estiverem carregados, e regenera o painel do Looker Studio.")

else:
    st.error("Senha incorreta.")
