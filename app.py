import streamlit as st
import pandas as pd
from bs4 import BeautifulSoup
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime
import re
import unicodedata
import time

# --- CONFIGURAÇÃO DA PÁGINA ---
st.set_page_config(page_title="Central de Relatórios WLM", layout="wide", page_icon="🔒")

# ID da sua planilha
ID_PLANILHA_MESTRA = "1XibBlm2x46Dk5bf4JvfrMepD4gITdaOtTALSgaFcwV0"

# --- FUNÇÕES AUXILIARES ---
def remover_acentos(texto):
    return ''.join(c for c in unicodedata.normalize('NFD', texto) if unicodedata.category(c) != 'Mn')

def conectar_sheets():
    scope = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
    credentials_dict = st.secrets["gcp_service_account"]
    creds = Credentials.from_service_account_info(credentials_dict, scopes=scope)
    client = gspread.authorize(creds)
    return client

# --- FUNÇÃO DE SEGURANÇA ---
def verificar_acesso():
    try:
        client = conectar_sheets()
        sh = client.open_by_key(ID_PLANILHA_MESTRA)
        try:
            ws_config = sh.worksheet("Config")
            senha_real = ws_config.acell('B1').value
            return senha_real
        except:
            ws_config = sh.add_worksheet(title="Config", rows=10, cols=5)
            ws_config.update('A1', 'Senha Mestra')
            ws_config.update('B1', 'admin')
            return 'admin'
    except Exception as e:
        return None

# --- NOVA FUNÇÃO INTELIGENTE: UPSERT (ATUALIZAÇÃO) ---
def salvar_com_upsert(nome_aba, novos_dados_df, colunas_chaves):
    """
    1. Lê a aba atual.
    2. Junta com os novos dados.
    3. Remove duplicatas baseadas nas chaves (Data + Técnico), mantendo o ÚLTIMO (novo).
    4. Reescreve a aba.
    """
    client = conectar_sheets()
    sh = client.open_by_key(ID_PLANILHA_MESTRA)
    
    # Tenta abrir a aba, se não existir, cria
    try:
        ws = sh.worksheet(nome_aba)
        dados_antigos = ws.get_all_records()
        df_antigo = pd.DataFrame(dados_antigos)
    except:
        ws = sh.add_worksheet(title=nome_aba, rows=1000, cols=20)
        df_antigo = pd.DataFrame()

    # Se a planilha antiga não estiver vazia, converte tudo para string para garantir comparação
    if not df_antigo.empty:
        for col in df_antigo.columns:
            df_antigo[col] = df_antigo[col].astype(str)
            
    # Converte novos dados para string também
    for col in novos_dados_df.columns:
        novos_dados_df[col] = novos_dados_df[col].astype(str)

    # Concatena (Antigo + Novo)
    df_total = pd.concat([df_antigo, novos_dados_df])

    # REMOVE DUPLICATAS (O Segredo)
    # keep='last' significa: se achar repetido, mantenha o que entrou por último (o novo upload)
    df_final = df_total.drop_duplicates(subset=colunas_chaves, keep='last')

    # Limpa e Salva
    ws.clear()
    ws.update([df_final.columns.values.tolist()] + df_final.values.tolist())
    
    return len(df_final)

# --- O MOTOR DE UNIFICAÇÃO (MANTIDO) ---
def processar_unificacao():
    try:
        client = conectar_sheets()
        sh = client.open_by_key(ID_PLANILHA_MESTRA)
        try:
            ws_com = sh.worksheet("Comissoes")
            ws_aprov = sh.worksheet("Aproveitamento")
        except: return False

        dados_com = ws_com.get_all_records()
        dados_aprov = ws_aprov.get_all_records()

        if not dados_com or not dados_aprov: return False

        df_com = pd.DataFrame(dados_com)
        df_aprov = pd.DataFrame(dados_aprov)

        # Limpeza e Padronização
        df_com.columns = [c.strip() for c in df_com.columns]
        df_aprov.columns = [c.strip() for c in df_aprov.columns]
        
        # Converte tudo para string para evitar erros de merge
        df_com = df_com.astype(str)
        df_aprov = df_aprov.astype(str)

        renomear_comissao = {"Data Processamento": "Data", "Sigla Técnico": "Técnico"}
        df_com.rename(columns=renomear_comissao, inplace=True)

        colunas_uteis_comissao = ['Data', 'Técnico', 'Horas Vendidas']
        df_com = df_com[[c for c in colunas_uteis_comissao if c in df_com.columns]]
        
        colunas_uteis_aprov = ['Data', 'Técnico', 'Disp', 'TP', 'TG']
        df_aprov = df_aprov[[c for c in colunas_uteis_aprov if c in df_aprov.columns]]

        df_final = pd.merge(df_com, df_aprov, on=['Data', 'Técnico'], how='outer', suffixes=('_Com', '_Aprov'))
        df_final.fillna("", inplace=True)

        try: ws_final = sh.worksheet("Consolidado")
        except: ws_final = sh.add_worksheet(title="Consolidado", rows=1000, cols=20)
        
        ws_final.clear()
        ws_final.update([df_final.columns.values.tolist()] + df_final.values.tolist())
        return True
    except Exception as e: 
        print(f"Erro: {e}")
        return False

# ============================================
# 🔒 INTERFACE
# ============================================

st.sidebar.image("https://cdn-icons-png.flaticon.com/512/3064/3064197.png", width=50)
st.sidebar.title("Login Seguro")

senha_digitada = st.sidebar.text_input("Digite a senha de acesso:", type="password")
senha_correta = verificar_acesso()

if senha_digitada == senha_correta:
    st.sidebar.success("✅ Acesso Liberado")
    
    st.title("🏭 Central de Processamento de Relatórios")
    
    aba_comissoes, aba_aproveitamento = st.tabs(["💰 Pagamento de Comissões", "⚙️ Aproveitamento Técnico"])

    # --- TAB 1: COMISSÕES ---
    with aba_comissoes:
        st.header("Processador de Comissões")
        st.info("💡 Lógica de Substituição Ativa: Se você carregar um relatório com Data e Técnico já existentes, o sistema atualizará os valores antigos.")
        arquivos_comissao = st.file_uploader("Upload Comissões HTML", type=["html", "htm"], accept_multiple_files=True, key="uploader_comissao")
        
        if arquivos_comissao:
            dados_comissao = []
            st.write(f"📂 Processando {len(arquivos_comissao)} arquivos...")
            for arquivo in arquivos_comissao:
                try:
                    try: conteudo = arquivo.read().decode("utf-8")
                    except: 
                        arquivo.seek(0)
                        conteudo = arquivo.read().decode("latin-1")
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
                                    dados_comissao.append([data_relatorio, arquivo.name, tecnico_atual, txt.replace("HORAS", "").strip()])
                                    break 
                except Exception as e: st.error(f"Erro: {e}")

            if len(dados_comissao) > 0:
                colunas_comissao = ["Data Processamento", "Nome do Arquivo", "Sigla Técnico", "Horas Vendidas"]
                df_comissao = pd.DataFrame(dados_comissao, columns=colunas_comissao)
                st.dataframe(df_comissao)
                
                if st.button("💾 Gravar e Atualizar Base (Comissões)", key="btn_comissao"):
                    progresso = st.progress(0, text="Iniciando...")
                    try:
                        progresso.progress(20, text="Lendo base atual e verificando duplicatas...")
                        
                        # --- CHAMA A FUNÇÃO INTELIGENTE ---
                        # Chaves para identificar duplicidade: Data e Técnico
                        qtd_final = salvar_com_upsert("Comissoes", df_comissao, ["Data Processamento", "Sigla Técnico"])
                        
                        progresso.progress(70, text=f"Base atualizada ({qtd_final} registros). Recalculando unificação...")
                        processar_unificacao()
                        
                        progresso.progress(100, text="Concluído!")
                        st.success("✅ Sucesso! Dados atualizados (duplicatas substituídas) e Relatório Consolidado pronto.")
                        st.balloons()
                    except Exception as e: st.error(f"Erro: {e}")

    # --- TAB 2: APROVEITAMENTO ---
    with aba_aproveitamento:
        st.header("Extrator de Aproveitamento")
        st.info("💡 Lógica de Substituição Ativa: Dados da mesma Data e Técnico serão sobrescritos pelos mais recentes.")
        arquivos_aprov = st.file_uploader("Upload Aproveitamento HTML", type=["html", "htm"], accept_multiple_files=True, key="uploader_aprov")
        if arquivos_aprov:
            dados_aprov = []
            for arquivo in arquivos_aprov:
                try:
                    raw_data = arquivo.read()
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
                                        dados_aprov.append([txt_cel0.split()[0], arquivo.name, tecnico_atual_aprov, 
                                                          celulas[1].get_text(strip=True), celulas[2].get_text(strip=True), celulas[3].get_text(strip=True)])
                                except: continue
                except Exception as e: st.error(f"Erro leitura: {e}")

            if len(dados_aprov) > 0:
                colunas_aprov = ["Data", "Arquivo", "Técnico", "Disp", "TP", "TG"]
                df_aprov = pd.DataFrame(dados_aprov, columns=colunas_aprov)
                st.dataframe(df_aprov)
                
                if st.button("💾 Gravar e Atualizar Base (Aproveitamento)", key="btn_aprov"):
                    progresso = st.progress(0, text="Iniciando...")
                    try:
                        progresso.progress(20, text="Lendo base atual e verificando duplicatas...")
                        
                        # --- CHAMA A FUNÇÃO INTELIGENTE ---
                        # Chaves: Data e Técnico
                        qtd_final = salvar_com_upsert("Aproveitamento", df_aprov, ["Data", "Técnico"])
                        
                        progresso.progress(70, text=f"Base atualizada ({qtd_final} registros). Recalculando unificação...")
                        processar_unificacao()
                        
                        progresso.progress(100, text="Concluído!")
                        st.success("✅ Sucesso! Dados atualizados e Relatório Consolidado pronto.")
                        st.balloons()
                    except Exception as e: st.error(f"Erro: {e}")

elif senha_digitada == "":
    st.info("👈 Digite a senha na barra lateral.")
else:
    st.error("🔒 Senha incorreta.")
