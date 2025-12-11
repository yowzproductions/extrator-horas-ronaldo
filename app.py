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
ID_PLANILHA_MESTRA = "1XibBlm2x46Dk5bf4JvfrMepD4gITdaOtTALSgaFcwV0"

# --- AUXILIARES ---
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
    BLINDAGEM DE DADOS:
    Recebe qualquer coisa ("8,30", "1.200,50", 8.3) e devolve FLOAT PURO (8.3).
    """
    if pd.isna(valor) or valor == "": 
        return 0.0
    
    if isinstance(valor, (int, float)): 
        return float(valor)
    
    # 1. Limpeza pesada de texto (tira espaços invisíveis do HTML)
    valor_str = str(valor).strip()
    valor_str = valor_str.replace('\xa0', '').replace('R$', '').strip()

    if not valor_str:
        return 0.0

    # 2. Lógica Brasileira
    # Se tem ponto e vírgula (Ex: 1.200,50), o ponto é milhar -> Removemos
    if '.' in valor_str and ',' in valor_str: 
        valor_str = valor_str.replace('.', '')
    
    # 3. A Vírgula vira Ponto (Ex: 8,30 -> 8.30) para o Python entender
    valor_str = valor_str.replace(',', '.')

    try: 
        return float(valor_str)
    except: 
        return 0.0

# OBS: REMOVI A FUNÇÃO 'float_para_string_br'. 
# NÃO VAMOS MAIS TRANSFORMAR EM TEXTO. ENVIAREMOS NÚMEROS.

def verificar_acesso():
    try:
        client = conectar_sheets()
        sh = client.open_by_key(ID_PLANILHA_MESTRA)
        try: return sh.worksheet("Config").acell('B1').value
        except: return 'admin'
    except: return None

# --- PARSERS (LEITURA DOS ARQUIVOS) ---
def parse_comissoes(arquivos):
    dados = []
    for arquivo in arquivos:
        try:
            arquivo.seek(0)
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
                        # Extrai apenas se parecer número
                        if "HORAS" in txt and any(c.isdigit() for c in txt) and "VENDIDAS" not in txt:
                            valor_limpo = txt.replace("HORAS", "").strip()
                            dados.append([data_relatorio, arquivo.name, tecnico_atual, valor_limpo])
                            break 
        except Exception as e: st.error(f"Erro no arquivo {arquivo.name}: {e}")
    return dados

def parse_aproveitamento(arquivos):
    dados = []
    for arquivo in arquivos:
        try:
            arquivo.seek(0)
            try: conteudo = arquivo.read().decode("utf-8")
            except:
                try: conteudo = arquivo.read().decode("latin-1")
                except: conteudo = arquivo.read().decode("utf-16")
            
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
                                dados.append([
                                    txt_cel0.split()[0], 
                                    arquivo.name, 
                                    tecnico_atual_aprov, 
                                    celulas[1].get_text(strip=True), 
                                    celulas[2].get_text(strip=True), 
                                    celulas[3].get_text(strip=True)
                                ])
                        except: continue
        except Exception as e: st.error(f"Erro no arquivo {arquivo.name}: {e}")
    return dados

# --- GRAVAÇÃO (SEM MAQUIAGEM DE TEXTO) ---
def atualizar_planilha_preservando_formato(sh, nome_aba, df_final):
    try:
        ws = sh.worksheet(nome_aba)
    except:
        ws = sh.add_worksheet(title=nome_aba, rows=2000, cols=20)

    # 1. Cabeçalho
    if not ws.get_all_values():
        ws.update('A1', [df_final.columns.values.tolist()])
        try: ws.format('A1:Z1', {'textFormat': {'bold': True}})
        except: pass

    # 2. Limpa Dados
    ws.batch_clear(["A2:Z10000"])

    # 3. Prepara Dados: ATENÇÃO AQUI
    # Trocamos NaN por 0.0 (número) e NÃO convertemos para string.
    df_final = df_final.fillna(0.0)
    
    # 4. Envia RAW DATA (Números Puros)
    # O Python manda 8.3 -> O Google recebe 8.3 -> O Google exibe 8,30 (se configurado)
    dados_para_enviar = df_final.values.tolist()
    
    if dados_para_enviar:
        ws.update('A2', dados_para_enviar)
        
    return True

# --- UPSERT ---
def salvar_com_upsert(nome_aba, novos_dados_df, colunas_chaves):
    client = conectar_sheets()
    sh = client.open_by_key(ID_PLANILHA_MESTRA)
    
    try:
        ws = sh.worksheet(nome_aba)
        dados_antigos = ws.get_all_records()
        df_antigo = pd.DataFrame(dados_antigos)
    except:
        df_antigo = pd.DataFrame()

    if not df_antigo.empty:
        for col in df_antigo.columns: df_antigo[col] = df_antigo[col].astype(str)
    for col in novos_dados_df.columns: novos_dados_df[col] = novos_dados_df[col].astype(str)

    df_total = pd.concat([df_antigo, novos_dados_df])
    df_final = df_total.drop_duplicates(subset=colunas_chaves, keep='last')
    
    # Nota: Aqui salvamos como String porque é armazenamento intermediário.
    # A mágica da conversão numérica acontece na UNIFICAÇÃO.
    atualizar_planilha_preservando_formato(sh, nome_aba, df_final)
    return len(df_final)

# --- UNIFICAÇÃO (CORRIGIDA - MODO MATEMÁTICO) ---
def processar_unificacao():
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

        # Limpeza
        df_com.columns = [c.strip() for c in df_com.columns]
        df_aprov.columns = [c.strip() for c in df_aprov.columns]
        renomear_comissao = {"Data Processamento": "Data", "Sigla Técnico": "Técnico"}
        df_com.rename(columns=renomear_comissao, inplace=True)

        # Seleção
        cols_com = ['Data', 'Técnico', 'Horas Vendidas']
        df_com = df_com[[c for c in cols_com if c in df_com.columns]]
        cols_aprov = ['Data', 'Técnico', 'Disp', 'TP', 'TG']
        df_aprov = df_aprov[[c for c in cols_aprov if c in df_aprov.columns]]

        # --- AQUI É O SEGREDO ---
        # Convertemos TUDO para FLOAT PYTHON (8.3)
        cols_numericas = ['Horas Vendidas', 'Disp', 'TP', 'TG']
        for col in cols_numericas:
            if col in df_com.columns: df_com[col] = df_com[col].apply(converter_br_para_float)
            if col in df_aprov.columns: df_aprov[col] = df_aprov[col].apply(converter_br_para_float)

        # Chaves
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
        df_final.fillna(0.0, inplace=True) # Zeros NUMÉRICOS
        
        # Consolidar
        df_final['Data'] = df_final.apply(lambda x: x['Data_C'] if x['Data_C'] != 0 and str(x['Data_C']) != "0" else x['Data_A'], axis=1)
        df_final['Técnico'] = df_final.apply(lambda x: x['Técnico_C'] if x['Técnico_C'] != 0 and str(x['Técnico_C']) != "0" else x['Técnico_A'], axis=1)

        cols_finais = ['Data', 'Técnico', 'Horas Vendidas', 'Disp', 'TP', 'TG']
        df_final = df_final[[c for c in cols_finais if c in df_final.columns]]

        # --- CRUCIAL: NÃO CONVERTER DE VOLTA PARA STRING ---
        # Removemos o loop que estragava os dados transformando em texto.
        # Enviamos o float direto.
        
        atualizar_planilha_preservando_formato(sh, "Consolidado", df_final)
        return True
    except Exception as e:
        print(f"Erro unificação: {e}")
        return False

# --- ROTINA MESTRA ---
def executar_rotina_global(df_com=None, df_aprov=None):
    status_msg = st.empty()
    bar = st.progress(0)
    try:
        if df_com is not None and not df_com.empty:
            status_msg.info("💾 Salvando Comissões...")
            salvar_com_upsert("Comissoes", df_com, ["Data Processamento", "Sigla Técnico"])
            bar.progress(40)
        
        if df_aprov is not None and not df_aprov.empty:
            status_msg.info("💾 Salvando Aproveitamento...")
            salvar_com_upsert("Aproveitamento", df_aprov, ["Data", "Técnico"])
            bar.progress(70)
            
        status_msg.info("🔄 Unificando bases...")
        sucesso = processar_unificacao()
        bar.progress(100)
        
        if sucesso:
            status_msg.success("✅ Sucesso! Dados Consolidados (Números Puros).")
            st.balloons()
        else:
            status_msg.warning("⚠️ Salvo, mas erro na unificação.")
            
    except Exception as e: status_msg.error(f"Erro: {e}")

# --- INTERFACE ---
st.sidebar.title("Login Seguro")
senha = st.sidebar.text_input("Senha:", type="password")

if senha == verificar_acesso():
    st.sidebar.success("Acesso Liberado")
    st.title("🏭 Central de Processamento WLM")
    
    aba1, aba2 = st.tabs(["💰 Comissões", "⚙️ Aproveitamento"])
    df_comissao_global = None
    df_aprov_global = None

    with aba1:
        st.header("Upload Comissões")
        files_com = st.file_uploader("Arquivos HTML", accept_multiple_files=True, key="up_com")
        if files_com:
            dados_c = parse_comissoes(files_com)
            if dados_c:
                df_comissao_global = pd.DataFrame(dados_c, columns=["Data Processamento", "Nome do Arquivo", "Sigla Técnico", "Horas Vendidas"])
                st.dataframe(df_comissao_global, height=200)

    with aba2:
        st.header("Upload Aproveitamento")
        files_aprov = st.file_uploader("Arquivos HTML/SLK", accept_multiple_files=True, key="up_aprov")
        if files_aprov:
            dados_a = parse_aproveitamento(files_aprov)
            if dados_a:
                df_aprov_global = pd.DataFrame(dados_a, columns=["Data", "Arquivo", "Técnico", "Disp", "TP", "TG"])
                st.dataframe(df_aprov_global, height=200)

    st.divider()
    col_btn, col_txt = st.columns([1, 4])
    with col_btn:
        if st.button("🚀 GRAVAR TUDO E ATUALIZAR", type="primary"):
            if df_comissao_global is None and df_aprov_global is None: st.warning("Sem arquivos.")
            else: executar_rotina_global(df_comissao_global, df_aprov_global)
else:
    if senha: st.error("Senha incorreta.")
