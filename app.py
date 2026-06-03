import streamlit as st
import pandas as pd
from bs4 import BeautifulSoup
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime
import re
import unicodedata
import time
import traceback

st.set_page_config(page_title="Central de Relatórios WLM", layout="wide", page_icon="🔒")
ID_PLANILHA_MESTRA = "1XibBlm2x46Dk5bf4JvfrMepD4gITdaOtTALSgaFcwV0"

def remover_acentos(texto):
    return ''.join(c for c in unicodedata.normalize('NFD', texto) if unicodedata.category(c) != 'Mn')

def conectar_sheets():
    scope = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
    creds = Credentials.from_service_account_info(st.secrets["gcp_service_account"], scopes=scope)
    return gspread.authorize(creds)

def padronizar_data(data_str):
    """Garante que a data sempre saia como dd/mm/yyyy — aceita dd/mm/yy ou dd/mm/yyyy."""
    if not data_str or str(data_str).strip() in ('', 'nan', 'None'):
        return ""
    s = str(data_str).strip()
    if '/' in s:
        partes = s.split('/')
        if len(partes) == 3:
            dia, mes, ano = partes
            if len(ano) == 2:
                ano = '20' + ano
            return f"{dia.zfill(2)}/{mes.zfill(2)}/{ano}"
    return s

def converter_br_para_float(valor):
    if pd.isna(valor) or str(valor).strip() in ('', 'nan'):
        return 0.0
    if isinstance(valor, (int, float)):
        return float(valor)
    s = str(valor).strip().replace('\xa0', '').replace('R$', '').strip()
    if not s:
        return 0.0
    if '.' in s and ',' in s:
        s = s.replace('.', '')
    s = s.replace(',', '.')
    try:
        return float(s)
    except:
        return 0.0

def verificar_acesso():
    try:
        client = conectar_sheets()
        sh = client.open_by_key(ID_PLANILHA_MESTRA)
        try:
            return sh.worksheet("Config").acell('B1').value
        except:
            return 'admin'
    except:
        return None

def obter_ultima_atualizacao():
    try:
        client = conectar_sheets()
        sh = client.open_by_key(ID_PLANILHA_MESTRA)
        datas = sh.worksheet("Consolidado").col_values(1)[1:]
        if not datas:
            return "Nenhuma"
        datas_dt = pd.to_datetime(datas, dayfirst=True, errors='coerce')
        return datas_dt.max().strftime('%d/%m/%Y')
    except:
        return "Erro ao ler"

# ---------------------------------------------------------------
# PARSERS
# ---------------------------------------------------------------
def parse_comissoes(arquivos):
    dados = []
    for arquivo in arquivos:
        try:
            arquivo.seek(0)
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
            for linha in soup.find_all("tr"):
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
                        txt = celula.get_text(strip=True).upper()
                        if "HORAS" in txt and any(c.isdigit() for c in txt) and "VENDIDAS" not in txt:
                            dados.append([data_relatorio, arquivo.name, tecnico_atual, txt.replace("HORAS", "").strip()])
                            break
        except Exception as e:
            st.error(f"Erro em {arquivo.name}: {e}")
    return dados

def parse_aproveitamento(arquivos):
    """
    Parser corrigido: busca as colunas Disp/TP/TG pelo cabeçalho da tabela,
    não por índice fixo — evita pegar a coluna errada quando o HTML tem
    colunas extras ou ordem diferente.
    """
    dados = []
    for arquivo in arquivos:
        try:
            arquivo.seek(0)
            try:
                conteudo = arquivo.read().decode("utf-8")
            except:
                try:
                    conteudo = arquivo.read().decode("latin-1")
                except:
                    conteudo = arquivo.read().decode("utf-16")

            soup = BeautifulSoup(conteudo, "html.parser")
            tecnico_atual = None

            # Descobre índices das colunas pelo cabeçalho
            idx_disp = idx_tp = idx_tg = None

            for linha in soup.find_all("tr"):
                celulas = linha.find_all(["td", "th"])
                textos = [remover_acentos(c.get_text(strip=True).upper()) for c in celulas]
                texto_linha = " ".join(textos)
                texto_original = linha.get_text(separator=" ", strip=True).upper()
                texto_limpo = remover_acentos(texto_original)

                if "TOTAL FILIAL:" in texto_original:
                    break

                # Detecta linha de cabeçalho com Disp/TP/TG
                if idx_disp is None:
                    for i, t in enumerate(textos):
                        if "DISP" in t:
                            idx_disp = i
                        if t in ("TP", "T.P", "T.P.", "TEMPO PADRAO", "TEMPO PADRÃO"):
                            idx_tp = i
                        if t in ("TG", "T.G", "T.G.", "TEMPO GARANTIA"):
                            idx_tg = i

                # Detecta técnico
                if "MECANICO" in texto_limpo and "TOT.MEC" not in texto_limpo:
                    try:
                        parte = texto_limpo.split("MECANICO")[1].replace(":", "").strip()
                        tecnico_atual = parte.split("-")[0].strip() if "-" in parte else parte.split()[0]
                    except:
                        continue

                if "TOT.MEC.:" in texto_original:
                    tecnico_atual = None
                    continue

                # Linha de dado: começa com data dd/mm/yy
                if tecnico_atual and celulas:
                    txt0 = celulas[0].get_text(strip=True)
                    if re.match(r"\d{2}/\d{2}/\d{2}", txt0):
                        data_val = txt0.split()[0]
                        try:
                            # Usa índices descobertos pelo cabeçalho; fallback para [1],[2],[3]
                            i_d = idx_disp if idx_disp is not None else 1
                            i_t = idx_tp   if idx_tp   is not None else 2
                            i_g = idx_tg   if idx_tg   is not None else 3

                            if len(celulas) > max(i_d, i_t, i_g):
                                disp = celulas[i_d].get_text(strip=True)
                                tp   = celulas[i_t].get_text(strip=True)
                                tg   = celulas[i_g].get_text(strip=True)
                                dados.append([data_val, arquivo.name, tecnico_atual, disp, tp, tg])
                        except:
                            continue
        except Exception as e:
            st.error(f"Erro em {arquivo.name}: {e}")
    return dados

# ---------------------------------------------------------------
# GRAVAÇÃO
# ---------------------------------------------------------------
def atualizar_planilha_preservando_formato(sh, nome_aba, df):
    try:
        ws = sh.worksheet(nome_aba)
    except:
        ws = sh.add_worksheet(title=nome_aba, rows=2000, cols=20)
    if not ws.get_all_values():
        ws.update('A1', [df.columns.tolist()])
        try:
            ws.format('A1:Z1', {'textFormat': {'bold': True}})
        except:
            pass
    ws.batch_clear(["A2:Z10000"])
    df = df.fillna(0.0)
    dados = df.values.tolist()
    if dados:
        ws.update('A2', dados)
    return True

def salvar_com_upsert(nome_aba, novos_df, colunas_chaves):
    client = conectar_sheets()
    sh = client.open_by_key(ID_PLANILHA_MESTRA)
    try:
        time.sleep(1)
        ws = sh.worksheet(nome_aba)
        df_antigo = pd.DataFrame(ws.get_all_records())
    except:
        df_antigo = pd.DataFrame()

    for col in colunas_chaves:
        if col in novos_df.columns:
            novos_df[col] = novos_df[col].astype(str).str.strip().str.upper()
        if not df_antigo.empty and col in df_antigo.columns:
            df_antigo[col] = df_antigo[col].astype(str).str.strip().str.upper()

    if not df_antigo.empty:
        col_data = 'Data Processamento' if 'Data Processamento' in novos_df.columns else 'Data'
        if col_data in df_antigo.columns:
            datas_novas = novos_df[col_data].unique()
            df_antigo = df_antigo[~df_antigo[col_data].isin(datas_novas)]
        df_final = pd.concat([df_antigo, novos_df], ignore_index=True)
    else:
        df_final = novos_df

    df_final = df_final.fillna(0.0)
    time.sleep(1.5)
    atualizar_planilha_preservando_formato(sh, nome_aba, df_final)
    return len(df_final)

def salvar_ajuste_manual(data, tecnico, metrica, valor, motivo):
    client = conectar_sheets()
    sh = client.open_by_key(ID_PLANILHA_MESTRA)
    try:
        ws = sh.worksheet("Ajustes")
    except:
        ws = sh.add_worksheet(title="Ajustes", rows=1000, cols=10)
        ws.append_row(["Data", "Técnico", "Métrica", "Valor", "Motivo", "Data do Registro"])
    ws.append_row([
        data.strftime('%d/%m/%Y'), tecnico, metrica, float(valor), motivo,
        datetime.now().strftime('%d/%m/%Y %H:%M:%S')
    ])

def aplicar_logica_ajustes(df):
    try:
        client = conectar_sheets()
        sh = client.open_by_key(ID_PLANILHA_MESTRA)
        dados = sh.worksheet("Ajustes").get_all_records()
        if not dados:
            return df
        df_aj = pd.DataFrame(dados)
        mapa = {
            "Horas Vendidas (HV)": "Horas Vendidas",
            "Tempo Padrão (TP)": "TP",
            "Tempo Disponível (Disp)": "Disp",
            "Tempo Garantia (TG)": "TG"
        }
        df['_dt'] = pd.to_datetime(df['Data'], dayfirst=True, errors='coerce')
        for _, row in df_aj.iterrows():
            try:
                dt = pd.to_datetime(row['Data'], dayfirst=True, errors='coerce')
                tec = str(row['Técnico']).strip()
                col = mapa.get(row['Métrica'])
                val = float(str(row['Valor']).replace(',', '.'))
                if col and col in df.columns:
                    mask = (df['_dt'] == dt) & (df['Técnico'] == tec)
                    if mask.any():
                        df.loc[mask, col] += val
            except:
                continue
        df.drop(columns=['_dt'], inplace=True)
        return df
    except Exception as e:
        print(f"Erro ajustes: {e}")
        return df

def aplicar_traducao_nomes(df):
    try:
        client = conectar_sheets()
        sh = client.open_by_key(ID_PLANILHA_MESTRA)
        linhas = sh.worksheet("Nomes").get_all_values()
        dic = {}
        for row in linhas[1:]:
            if len(row) >= 2 and row[0].strip() and row[1].strip():
                dic[row[0].strip().upper()] = row[1].strip()
        if dic:
            df['Técnico'] = df['Técnico'].apply(lambda s: dic.get(str(s).strip().upper(), s))
    except Exception as e:
        print(f"Tradução nomes: {e}")
    return df

# ---------------------------------------------------------------
# UNIFICAÇÃO — com correção de datas no Sheets antes do merge
# ---------------------------------------------------------------
def processar_unificacao():
    log = st.empty()

    def step(msg):
        log.info(msg)
        print(msg)

    def fail(msg):
        log.error(msg)
        print(msg)

    try:
        step("🔄 [1/8] Conectando ao Sheets...")
        client = conectar_sheets()
        sh = client.open_by_key(ID_PLANILHA_MESTRA)

        step("🔄 [2/8] Lendo Comissoes...")
        dados_com = sh.worksheet("Comissoes").get_all_records()

        step("🔄 [3/8] Lendo Aproveitamento...")
        dados_aprov = sh.worksheet("Aproveitamento").get_all_records()

        step(f"📊 Comissoes: {len(dados_com)} linhas | Aproveitamento: {len(dados_aprov)} linhas")

        if not dados_com and not dados_aprov:
            fail("❌ Ambas as abas estão vazias.")
            return False

        # --- Monta DataFrames ---
        df_com = pd.DataFrame(dados_com) if dados_com else pd.DataFrame(columns=['Data', 'Técnico', 'Horas Vendidas'])
        df_aprov = pd.DataFrame(dados_aprov) if dados_aprov else pd.DataFrame(columns=['Data', 'Técnico', 'Disp', 'TP', 'TG'])

        df_com.columns = [c.strip() for c in df_com.columns]
        df_aprov.columns = [c.strip() for c in df_aprov.columns]

        step(f"📋 Colunas Comissoes: {list(df_com.columns)}")
        step(f"📋 Colunas Aproveitamento: {list(df_aprov.columns)}")

        # Renomeia colunas de Comissoes
        df_com.rename(columns={"Data Processamento": "Data", "Sigla Técnico": "Técnico"}, inplace=True)

        # Filtra colunas necessárias
        df_com  = df_com[[c  for c in ['Data','Técnico','Horas Vendidas'] if c in df_com.columns]]
        df_aprov = df_aprov[[c for c in ['Data','Técnico','Disp','TP','TG'] if c in df_aprov.columns]]

        # Verifica colunas obrigatórias
        for col in ['Data', 'Técnico']:
            if col not in df_com.columns:
                fail(f"❌ Coluna '{col}' ausente em Comissoes. Colunas disponíveis: {list(df_com.columns)}")
                return False
            if col not in df_aprov.columns:
                fail(f"❌ Coluna '{col}' ausente em Aproveitamento. Colunas disponíveis: {list(df_aprov.columns)}")
                return False

        # --- CORREÇÃO CRÍTICA: padroniza datas que já estão no Sheets com 2 dígitos ---
        step("🔄 [4/8] Padronizando datas (corrigindo dd/mm/yy → dd/mm/yyyy)...")
        df_com['Data']   = df_com['Data'].apply(padronizar_data)
        df_aprov['Data'] = df_aprov['Data'].apply(padronizar_data)

        step(f"📅 Amostra datas Comissoes:     {df_com['Data'].dropna().head(3).tolist()}")
        step(f"📅 Amostra datas Aproveitamento: {df_aprov['Data'].dropna().head(3).tolist()}")

        # Converte numéricos
        step("🔄 [5/8] Convertendo valores numéricos...")
        for col in ['Horas Vendidas']:
            if col in df_com.columns:
                df_com[col] = df_com[col].apply(converter_br_para_float)
        for col in ['Disp', 'TP', 'TG']:
            if col in df_aprov.columns:
                df_aprov[col] = df_aprov[col].apply(converter_br_para_float)

        step(f"🔢 Amostra Disp Aproveitamento: {df_aprov['Disp'].head(5).tolist() if 'Disp' in df_aprov.columns else 'coluna ausente'}")

        # Chaves para merge
        df_com['_kd']   = df_com['Data'].astype(str).str.strip()
        df_com['_kt']   = df_com['Técnico'].astype(str).str.strip().str.upper()
        df_aprov['_kd'] = df_aprov['Data'].astype(str).str.strip()
        df_aprov['_kt'] = df_aprov['Técnico'].astype(str).str.strip().str.upper()

        step(f"🔑 Amostra chaves Comissoes:     {list(zip(df_com['_kd'].head(3), df_com['_kt'].head(3)))}")
        step(f"🔑 Amostra chaves Aproveitamento: {list(zip(df_aprov['_kd'].head(3), df_aprov['_kt'].head(3)))}")

        # Verifica interseção de chaves
        chaves_com   = set(zip(df_com['_kd'], df_com['_kt']))
        chaves_aprov = set(zip(df_aprov['_kd'], df_aprov['_kt']))
        intersecao   = chaves_com & chaves_aprov
        step(f"🔗 Chaves em comum: {len(intersecao)} | Só Comissoes: {len(chaves_com - chaves_aprov)} | Só Aproveitamento: {len(chaves_aprov - chaves_com)}")

        # Merge
        step("🔄 [6/8] Executando merge...")
        df_final = pd.merge(df_com, df_aprov, on=['_kd', '_kt'], how='outer', suffixes=('_C', '_A'))
        step(f"📊 Linhas após merge: {len(df_final)}")

        # Reconstrói Data e Técnico a partir das chaves (sem depender de combine_first)
        df_final['Data']    = df_final['_kd']
        df_final['Técnico'] = df_final['_kt']

        # Remove linhas sem chave
        df_final = df_final[
            df_final['Data'].notna() & (df_final['Data'].str.strip() != '') &
            df_final['Técnico'].notna() & (df_final['Técnico'].str.strip() != '')
        ]

        if df_final.empty:
            fail("❌ df_final ficou vazio após o merge. Nenhuma chave Data+Técnico em comum ou válida.")
            return False

        # Seleciona colunas finais
        cols_finais = ['Data', 'Técnico', 'Horas Vendidas', 'Disp', 'TP', 'TG']
        df_final = df_final[[c for c in cols_finais if c in df_final.columns]]

        # Garante tipos numéricos e aplica divisão por 100
        step("🔄 [7/8] Aplicando escala decimal (/100) e regras de negócio...")
        for col in ['Horas Vendidas', 'Disp', 'TP', 'TG']:
            if col in df_final.columns:
                df_final[col] = pd.to_numeric(df_final[col], errors='coerce').fillna(0.0) / 100.0

        df_final = aplicar_logica_ajustes(df_final)
        df_final = aplicar_traducao_nomes(df_final)

        step("🔄 [8/8] Gravando aba Consolidado...")
        atualizar_planilha_preservando_formato(sh, "Consolidado", df_final)

        log.success(f"✅ Consolidado gravado com {len(df_final)} linhas!")
        return True

    except Exception as e:
        fail(f"❌ Erro crítico: {e}\n\n{traceback.format_exc()}")
        return False

# ---------------------------------------------------------------
# ROTINA MESTRA
# ---------------------------------------------------------------
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
        status_msg.info("🔄 Unificando...")
        sucesso = processar_unificacao()
        bar.progress(100)
        if sucesso:
            status_msg.success("✅ Sucesso! Dados consolidados e enviados para o BI.")
            st.balloons()
        else:
            status_msg.warning("⚠️ Salvo, mas erro na unificação. Veja mensagens acima.")
    except Exception as e:
        status_msg.error(f"Erro: {e}\n\n{traceback.format_exc()}")

def listar_tecnicos_unicos():
    try:
        client = conectar_sheets()
        sh = client.open_by_key(ID_PLANILHA_MESTRA)
        try:
            vals = sh.worksheet("Consolidado").col_values(2)[1:]
        except:
            vals = []
        return sorted(set(v for v in vals if v))
    except:
        return []

# ---------------------------------------------------------------
# INTERFACE
# ---------------------------------------------------------------
st.sidebar.title("Login Seguro")
senha = st.sidebar.text_input("Senha:", type="password")

if senha == verificar_acesso():
    st.sidebar.success("Acesso Liberado")
    st.title("🏭 Central de Processamento WLM")
    st.info(f"📅 **Último dado processado na base:** {obter_ultima_atualizacao()}")

    aba1, aba2, aba3 = st.tabs(["💰 Comissões", "⚙️ Aproveitamento", "🔧 Ajustes Manuais"])
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
            else:
                st.warning("⚠️ Nenhum dado extraído do arquivo de Aproveitamento.")

    with aba3:
        st.header("Correção e Ajustes")
        with st.form("form_ajustes"):
            col_a, col_b = st.columns(2)
            data_adj = col_a.date_input("Data do Ajuste")
            lista_tec = listar_tecnicos_unicos()
            if not lista_tec:
                lista_tec = ["Digite Manualmente Abaixo"]
            tec_adj = col_b.selectbox("Técnico", lista_tec)
            tec_manual = st.text_input("Ou digite a Sigla (se não estiver na lista)")
            col_c, col_d = st.columns(2)
            metrica_adj = col_c.selectbox("Métrica", ["Horas Vendidas (HV)", "Tempo Padrão (TP)", "Tempo Disponível (Disp)", "Tempo Garantia (TG)"])
            valor_adj = col_d.number_input("Valor (+/-)", step=0.5, format="%.2f")
            motivo_adj = st.text_input("Motivo")
            if st.form_submit_button("💾 Salvar Ajuste e Atualizar BI"):
                tec_final = tec_manual.upper().strip() if tec_manual else tec_adj
                if tec_final:
                    salvar_ajuste_manual(data_adj, tec_final, metrica_adj, valor_adj, motivo_adj)
                    st.success(f"Ajuste salvo para {tec_final}!")
                    with st.spinner("Atualizando BI..."):
                        if processar_unificacao():
                            st.success("BI Atualizado!")
                else:
                    st.error("Selecione um técnico.")

        st.markdown("### Últimos Ajustes")
        try:
            client = conectar_sheets()
            sh = client.open_by_key(ID_PLANILHA_MESTRA)
            df_aj = pd.DataFrame(sh.worksheet("Ajustes").get_all_records())
            if not df_aj.empty:
                st.dataframe(df_aj.tail(5))
        except:
            st.write("Nenhum ajuste registrado.")

    st.divider()
    if st.button("🚀 GRAVAR TUDO E ATUALIZAR", type="primary"):
        if df_comissao_global is None and df_aprov_global is None:
            st.warning("Sem arquivos para processar.")
        else:
            executar_rotina_global(df_comissao_global, df_aprov_global)

else:
    if senha:
        st.error("Senha incorreta.")
