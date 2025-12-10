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

def converter_br_para_float(valor):
    """
    Transforma '8,30' (str) em 8.3 (float).
    Remove pontos de milhar e troca vírgula por ponto.
    """
    if pd.isna(valor) or valor == "":
        return 0.0
    
    # Se já for número (int ou float), retorna ele mesmo
    if isinstance(valor, (int, float)):
        return float(valor)
        
    valor_str = str(valor).strip()
    
    # Remove ponto de milhar se existir (ex: 1.000,00 -> 1000,00)
    # Cuidado: assumindo que ponto é milhar e virgula é decimal
    if '.' in valor_str and ',' in valor_str:
        valor_str = valor_str.replace('.', '')
    
    # Troca vírgula por ponto (8,3 -> 8.3)
    valor_str = valor_str.replace(',', '.')
    
    try:
        return float(valor_str)
    except:
        return 0.0

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
            return 'admin'
    except Exception as e:
        return None

# --- UPSERT (ATUALIZAÇÃO INTELIGENTE) ---
def salvar_com_upsert(nome_aba, novos_dados_df, colunas_chaves):
    client = conectar_sheets()
    sh = client.open_by_key(ID_PLANILHA_MESTRA)
    
    try:
        ws = sh.worksheet(nome_aba)
        dados_antigos = ws.get_all_records()
        df_antigo = pd.DataFrame(dados_antigos)
    except:
        ws = sh.add_worksheet(title=nome_aba, rows=1000, cols=20)
        df_antigo = pd.DataFrame()

    # Garante que tudo é string para comparar chaves corretamente
    if not df_antigo.empty:
        for col in df_antigo.columns:
            df_antigo[col] = df_antigo[col].astype(str)
            
    for col in novos_dados_df.columns:
        novos_dados_df[col] = novos_dados_df[col].astype(str)

    # Concatena e Remove Duplicatas
    df_total = pd.concat([df_antigo, novos_dados_df])
    df_final = df_total.drop_duplicates(subset=colunas_chaves, keep='last')

    ws.clear()
    ws.update([df_final.columns.values.tolist()] + df_final.values.tolist())
    
    return len(df_final)

# --- O MOTOR DE UNIFICAÇÃO (CORRIGIDO PARA NÚMEROS REAIS) ---
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

        # Limpeza de Nomes
        df_com.columns = [c.strip() for c in df_com.columns]
        df_aprov.columns = [c.strip() for c in df_aprov.columns]

        # Padronização das Chaves
        renomear_comissao = {"Data Processamento": "Data", "Sigla Técnico": "Técnico"}
        df_com.rename(columns=renomear_comissao, inplace=True)

        # Filtro de Colunas Úteis
        colunas_uteis_comissao = ['Data', 'Técnico', 'Horas Vendidas']
        # Filtra apenas se existirem
        cols_com_existentes = [c for c in colunas_uteis_comissao if c in df_com.columns]
        df_com = df_com[cols_com_existentes]
        
        colunas_uteis_aprov = ['Data', 'Técnico', 'Disp', 'TP', 'TG']
        cols_aprov_existentes = [c for c in colunas_uteis_aprov if c in df_aprov.columns]
        df_aprov = df_aprov[cols_aprov_existentes]

        # --- TRATAMENTO NUMÉRICO (A CORREÇÃO DO 8,30 -> 8.3) ---
        # Converte as colunas numéricas de texto BR para float Python
        
        # Lista de colunas que DEVEM ser números
        cols_numericas = ['Horas Vendidas', 'Disp', 'TP', 'TG']
        
        # Aplica na tabela Comissões
        for col in cols_numericas:
            if col in df_com.columns:
                df_com[col] = df_com[col].apply(converter_br_para_float)
                
        # Aplica na tabela Aproveitamento
        for col in cols_numericas:
            if col in df_aprov.columns:
                df_aprov[col] = df_aprov[col].apply(converter_br_para_float)

        # --- FIM DO TRATAMENTO NUMÉRICO ---

        # Preparar chaves para o Merge (Data e Técnico precisam ser String para cruzar)
        df_com['Data_Key'] = df_com['Data'].astype(str)
        df_com['Tecnico_Key'] = df_com['Técnico'].astype(str)
        
        df_aprov['Data_Key'] = df_aprov['Data'].astype(str)
        df_aprov['Tecnico_Key'] = df_aprov['Técnico'].astype(str)

        # Merge usando as chaves auxiliares
        df_final = pd.merge(
            df_com, 
            df_aprov, 
            left_on=['Data_Key', 'Tecnico_Key'],
            right_on=['Data_Key', 'Tecnico_Key'], 
            how='outer', 
            suffixes=('_Com', '_Aprov')
        )
        
        # Limpeza pós-merge
        df_final.fillna(0, inplace=True) # Preenche vazios numéricos com 0

        # Consolida as colunas de Data e Técnico (pega de um lado ou do outro)
        df_final['Data'] = df_final.apply(lambda x: x['Data_x'] if x['Data_x'] != 0 and x['Data_x'] != "0" else x['Data_y'], axis=1)
        df_final['Técnico'] = df_final.apply(lambda x: x['Técnico_x'] if x['Técnico_x'] != 0 and x['Técnico_x'] != "0" else x['Técnico_y'], axis=1)

        # Seleciona colunas finais limpas
        cols_finais = ['Data', 'Técnico', 'Horas Vendidas', 'Disp', 'TP', 'TG']
        # Garante que só pega colunas que existem no resultado
        cols_finais_validas = [c for c in cols_finais if c in df_final.columns]
        df_final = df_final[cols_finais_validas]

        # Salvar
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
        st.info("💡 Se houver duplicidade (Data + Técnico), o sistema mantém o dado mais recente.")
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
                        progresso.progress(20, text="Verificando duplicatas...")
                        qtd_final = salvar_com_upsert("Comissoes", df_comissao, ["Data Processamento", "Sigla Técnico"])
                        progresso.progress(70, text=f"Base atualizada. Recalculando unificação...")
                        processar_unificacao()
                        progresso.progress(100, text="Concluído!")
                        st.success(f"✅ Sucesso! Base atualizada e corrigida.")
                        st.balloons()
                    except Exception as e: st.error(f"Erro: {e}")

    # --- TAB 2: APROVEITAMENTO ---
    with aba_aproveitamento:
        st.header("Extrator de Aproveitamento")
        st.info("💡 Dados da mesma Data e Técnico serão sobrescritos pelos mais recentes.")
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
                        progresso.progress(20, text="Verificando duplicatas...")
                        qtd_final = salvar_com_upsert("Aproveitamento", df_aprov, ["Data", "Técnico"])
                        progresso.progress(70, text=f"Base atualizada. Recalculando unificação...")
                        processar_unificacao()
                        progresso.progress(100, text="Concluído!")
                        st.success("✅ Sucesso!")
                        st.balloons()
                    except Exception as e: st.error(f"Erro: {e}")

elif senha_digitada == "":
    st.info("👈 Digite a senha na barra lateral.")
else:
    st.error("🔒 Senha incorreta.")
