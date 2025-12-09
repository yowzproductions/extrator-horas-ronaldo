# --- FUNÇÃO DE PROCESSAMENTO DE DADOS (ETL - Versão Ajustada) ---
def processar_dados(arquivo1, arquivo2, data_ref):
    # 1. Conecta ao Banco
    planilha = conectar_google_sheets()
    
    # 2. Carrega o Glossário
    # ATENÇÃO: Confirme se o nome da aba é 'Glossario' ou 'Comissoes' conforme sua correção
    glossario_sheet = planilha.worksheet("Glossario") 
    df_glossario = pd.DataFrame(glossario_sheet.get_all_records())
    
    # Padroniza para evitar erros
    # Ajuste 'Nome_Completo' se sua coluna tiver outro nome
    if 'Nome_Completo' in df_glossario.columns:
        df_glossario['Nome_Completo'] = df_glossario['Nome_Completo'].astype(str).str.strip().str.upper()
    
    # 3. Função auxiliar para extrair dados "na unha" (linha a linha)
    # Isso resolve o problema de pegar o Total da Empresa errado
    def extrair_do_html(arquivo_html, termo_busca):
        df_raw = pd.read_html(arquivo_html)[0]
        # Converte tudo para texto para facilitar a busca
        df_raw = df_raw.astype(str)
        
        dados_extraidos = []
        tecnico_atual = None
        
        # Vamos varrer todas as células procurando os padrões
        for col in df_raw.columns:
            for valor in df_raw[col]:
                valor_limpo = valor.upper().strip()
                
                # A. Identifica o Técnico (A "Chave")
                if "TOTAL DO FUNCIONARIO" in valor_limpo:
                    # Pega o que vem depois de "FUNCIONARIO " (ex: MCV)
                    try:
                        tecnico_atual = valor_limpo.split("TOTAL DO FUNCIONARIO")[1].replace(":", "").strip()
                    except:
                        tecnico_atual = None
                
                # B. O FREIO DE MÃO (A Correção)
                # Se encontrarmos essas palavras, "esquecemos" o técnico atual
                if "TOTAL DA FILIAL" in valor_limpo or "TOTAL DA EMPRESA" in valor_limpo:
                    tecnico_atual = None
                
                # C. Pega o Valor (apenas se tivermos um técnico válido selecionado)
                if termo_busca.upper() in valor_limpo and tecnico_atual is not None:
                    # Geralmente o valor está na mesma célula ou precisamos limpar o texto
                    # Exemplo: "Horas Vendidas: 5,70 HORAS" -> queremos "5,70"
                    try:
                        # Pega apenas os números e a vírgula
                        apenas_numeros = valor_limpo.split(":")[-1].replace("HORAS", "").strip()
                        dados_extraidos.append({
                            "Sigla_Capturada": tecnico_atual,
                            "Valor": apenas_numeros
                        })
                    except:
                        pass
                        
        return pd.DataFrame(dados_extraidos)

    # 4. Processa os dois arquivos usando a nova lógica
    # Relatório 1: Busca "Tempo Padrão" (ajuste o termo se necessário)
    df1 = extrair_do_html(arquivo1, "TEMPO PADRÃO") 
    
    # Relatório 2: Busca "Horas Vendidas"
    df2 = extrair_do_html(arquivo2, "HORAS VENDIDAS")

    # Renomeia colunas para facilitar o cruzamento
    if not df1.empty:
        df1 = df1.rename(columns={"Valor": "Val1"})
    else:
        df1 = pd.DataFrame(columns=["Sigla_Capturada", "Val1"])

    if not df2.empty:
        df2 = df2.rename(columns={"Valor": "Val2"})
    else:
        df2 = pd.DataFrame(columns=["Sigla_Capturada", "Val2"])

    # 5. Cruza os dados extraídos (Relatório 1 + Relatório 2)
    df_full = pd.merge(df1, df2, on="Sigla_Capturada", how="outer")

    # 6. Cruza com o Glossário (Para pegar o Nome Completo)
    # O Glossário tem a coluna 'Sigla' e 'Nome_Completo'
    df_final = pd.merge(df_full, df_glossario, left_on="Sigla_Capturada", right_on="Sigla", how="left")

    # 7. Prepara para salvar
    registros = []
    for _, row in df_final.iterrows():
        # Se não achou no glossário, usa a sigla mesmo como nome
        nome = row['Nome_Completo'] if pd.notna(row['Nome_Completo']) else row['Sigla_Capturada']
        sigla = row['Sigla_Capturada']
        
        val1 = row['Val1'] if pd.notna(row['Val1']) else "0,00"
        val2 = row['Val2'] if pd.notna(row['Val2']) else "0,00"
        
        # Ordem das colunas no Sheets: [Data, Sigla, Nome, Tempo_Padrao, Hora_Vendida]
        registros.append([str(data_ref), sigla, nome, val1, val2])

    # 8. Escreve no Sheets
    if registros:
        hist_sheet = planilha.worksheet("Historico")
        hist_sheet.append_rows(registros)
    
    return len(registros)
