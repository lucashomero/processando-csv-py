import pandas as pd
import glob
import os

# --- Configurações Iniciais ---
pasta_dos_csvs = "./Dados"
nome_arquivo_consolidado = "Consolidado.csv"
nome_arquivo_resumo_metas = "ResumoMetas.csv"

# --- 1. Leitura e Consolidação dos CSVs (Gerar Consolidado.csv) ---
def consolidar_csvs(caminho_pasta, arquivo_saida):
    """
    Lê todos os arquivos CSV de uma pasta, consolida-os em um único DataFrame
    e salva em um novo arquivo CSV.
    """
    arquivos_csv = glob.glob(os.path.join(caminho_pasta, "teste_*.csv")) # Pega todos os arquivos que começam com 'teste_' e terminam com '.csv'
    
    if not arquivos_csv:
        print(f"Nenhum arquivo CSV encontrado na pasta: {caminho_pasta}")
        return None

    lista_de_dfs = []
    for arquivo in arquivos_csv:
        try:
            # Dica: Verifique as opções do read_csv conforme o seu arquivo [cite: 99]
            # Ex: separador, encoding
            df_temp = pd.read_csv(arquivo, sep=',', encoding='utf-8') # Ajuste sep e encoding se necessário
            # Adicionar uma coluna para identificar a origem, se útil (opcional)
            # df_temp['origem_arquivo'] = os.path.basename(arquivo)
            lista_de_dfs.append(df_temp)
            print(f"Arquivo {arquivo} lido com sucesso.")
        except Exception as e:
            print(f"Erro ao ler o arquivo {arquivo}: {e}")
            
    if not lista_de_dfs:
        print("Nenhum DataFrame para concatenar.")
        return None

    # Concatena todos os DataFrames da lista [cite: 100, 101]
    df_consolidado = pd.concat(lista_de_dfs, ignore_index=True)
    
    try:
        df_consolidado.to_csv(arquivo_saida, index=False, sep=',', encoding='utf-8') # Salva o CSV consolidado [cite: 107, 109]
        print(f"Arquivo consolidado '{arquivo_saida}' gerado com sucesso com {len(df_consolidado)} linhas.")
    except Exception as e:
        print(f"Erro ao salvar o arquivo consolidado: {e}")
        
    return df_consolidado

# --- 2. Cálculo das Metas ---
# As fórmulas das metas estão no documento (Capítulo 3)
# Exemplo: Meta 1 [cite: 38, 44, 51, 57, 61, 65, 68, 74]
# Meta1 = (Σ julgadom1 / (Σ cnm1 + Σ desm1 - Σ susm1)) * 100
# Onde cnm1 é 'casos_novos_2025', julgadom1 é 'julgados_2025', etc.

def calcular_meta1(df_tribunal):
    """Calcula a Meta 1 para um DataFrame de um tribunal específico."""
    # Assegure-se que as colunas existem e trate possíveis valores nulos (NaN)
    # que podem atrapalhar a soma. O .sum() do pandas já ignora NaN por padrão.
    soma_julgadom1 = df_tribunal['julgados_2025'].sum() # Nome da coluna conforme exemplo da Meta 1
    soma_cnm1 = df_tribunal['casos_novos_2025'].sum()
    soma_desm1 = df_tribunal['dessobrestados_2025'].sum()
    soma_susm1 = df_tribunal['suspensos_2025'].sum()
    
    denominador = soma_cnm1 + soma_desm1 - soma_susm1
    if denominador == 0:
        return "NA" # Ou 0, ou alguma outra indicação de divisão por zero
        
    meta1_resultado = (soma_julgadom1 / denominador) * 100
    return meta1_resultado

# Adicione funções para calcular as outras metas (Meta 2A, 2B, 4A, etc.)
# Lembre-se que cada ramo de justiça tem um conjunto diferente de metas e fórmulas. [cite: 38, 41, 44, 47, 48, 51, 53, 57, 60, 61, 65, 68, 70, 74, 76]

def processar_tribunais(df_dados_consolidados):
    """
    Processa os dados consolidados para calcular as metas de cada tribunal
    e retorna um DataFrame com os resultados.
    """
    if df_dados_consolidados is None:
        print("DataFrame consolidado está vazio. Não é possível processar tribunais.")
        return None

    # A coluna 'ramo_justica' identificará o tipo de justiça [cite: 91]
    # Você precisará agrupar ou iterar por tribunal.
    # Supondo que haja uma coluna 'sigla_tribunal' ou similar para agrupar.
    # Se não houver, você pode precisar inferir dos nomes dos arquivos ou outra coluna.
    
    resultados_metas = []

    # Exemplo de como iterar se você tiver uma coluna que identifica unicamente cada tribunal
    # Este é um PONTO CRÍTICO: você precisa de uma coluna para agrupar os dados POR TRIBUNAL
    # antes de aplicar as somas das metas. O `glob.glob` acima pega arquivos que parecem ser por tribunal.
    # Se cada arquivo CSV já é um tribunal, e a consolidação mantém isso distinguível
    # (ex: se 'sigla_tribunal' ou 'ramo_justica' + 'nome_tribunal' for único),
    # então você pode agrupar por essa(s) chave(s).

    # Supondo que 'sigla_tribunal' seja a coluna identificadora
    if 'sigla_tribunal' not in df_dados_consolidados.columns:
        print("Coluna 'sigla_tribunal' não encontrada. Adapte esta parte do código.")
        # Alternativa: se cada arquivo original representa um tribunal, você pode precisar
        # processar cada df_temp DENTRO do loop de consolidação_csvs antes de concatenar,
        # ou adicionar uma coluna 'nome_tribunal_origem' durante a leitura.
        return None # Ou implemente a lógica de agrupamento correta

    for tribunal_nome, df_tribunal in df_dados_consolidados.groupby('sigla_tribunal'):
        print(f"Processando tribunal: {tribunal_nome}")
        
        ramo_justica = df_tribunal['ramo_justica'].iloc[0] # Pega o ramo de justiça do tribunal
        
        desempenho_tribunal = {'tribunal': tribunal_nome, 'ramo_justica': ramo_justica}
        
        # --- Aqui você chamaria as funções de cálculo de meta ---
        # --- de acordo com o `ramo_justica` ---
        
        # Exemplo genérico (PRECISA SER ADAPTADO POR RAMO DE JUSTIÇA)
        desempenho_tribunal['Meta1'] = calcular_meta1(df_tribunal.copy()) # Envia uma cópia para evitar SettingWithCopyWarning

        # Adicione outras metas aqui. Ex:
        # if ramo_justica == "Justiça Estadual":
        #     desempenho_tribunal['Meta2A_JE'] = calcular_meta2a_je(df_tribunal.copy())
        #     desempenho_tribunal['Meta4A_JE'] = calcular_meta4a_je(df_tribunal.copy())
        # elif ramo_justica == "Justiça do Trabalho":
        #     desempenho_tribunal['Meta2A_JT'] = calcular_meta2a_jt(df_tribunal.copy())
        #     # ... e assim por diante
        # else: # Lógica para outros ramos ou metas comuns
        #     pass
            
        # Preencher com 'NA' as metas não aplicáveis [cite: 87]
        # (Isso pode ser feito definindo um conjunto de todas as colunas de metas possíveis
        # e preenchendo as que não foram calculadas com 'NA')

        resultados_metas.append(desempenho_tribunal)
        
    df_resultados_metas = pd.DataFrame(resultados_metas)
    
    # Preencher colunas de metas faltantes com "NA"
    # Ex: todas_as_colunas_de_metas = ['Meta1', 'Meta2A_JE', 'Meta2A_JT', ...]
    # for col in todas_as_colunas_de_metas:
    #    if col not in df_resultados_metas.columns:
    #        df_resultados_metas[col] = "NA"
            
    try:
        df_resultados_metas.to_csv(nome_arquivo_resumo_metas, index=False, sep=',', encoding='utf-8')
        print(f"Arquivo de resumo de metas '{nome_arquivo_resumo_metas}' gerado com sucesso.")
    except Exception as e:
        print(f"Erro ao salvar o arquivo de resumo de metas: {e}")
        
    return df_resultados_metas

# --- 3. Geração de Gráficos ---
def gerar_graficos(df_resumo):
    """
    Gera gráficos comparativos do desempenho dos tribunais.
    (Esta é uma tarefa mais elaborada, comece com algo simples)
    """
    if df_resumo is None or df_resumo.empty:
        print("DataFrame de resumo vazio. Não é possível gerar gráficos.")
        return

    try:
        import matplotlib.pyplot as plt
        # Exemplo: Gráfico de barras para a Meta 1 para alguns tribunais
        # df_resumo_filtrado = df_resumo.dropna(subset=['Meta1']) # Remove NAs para o gráfico
        # df_resumo_filtrado = df_resumo_filtrado[df_resumo_filtrado['Meta1'] != 'NA']
        # df_resumo_filtrado['Meta1'] = pd.to_numeric(df_resumo_filtrado['Meta1'])
        
        # df_resumo_filtrado.head(10).plot(kind='bar', x='tribunal', y='Meta1')
        # plt.title('Desempenho Meta 1 (Top 10 Tribunais)')
        # plt.ylabel('Percentual Atingido (%)')
        # plt.xticks(rotation=45, ha="right")
        # plt.tight_layout()
        # plt.savefig("grafico_meta1.png")
        # print("Gráfico 'grafico_meta1.png' salvo.")
        print("Funcionalidade de gráfico a ser implementada.")

    except ImportError:
        print("Biblioteca matplotlib não encontrada. Instale com 'pip install matplotlib'")
    except Exception as e:
        print(f"Erro ao gerar gráfico: {e}")

# --- Função Principal (Main) ---
if __name__ == "__main__":
    print("Iniciando processamento...")
    
    # 1. Consolidar todos os CSVs de dados
    df_consolidado = consolidar_csvs(pasta_dos_csvs, nome_arquivo_consolidado)
    
    # 2. Calcular as metas para cada tribunal
    # df_consolidado_teste = pd.read_csv(nome_arquivo_consolidado) # Se quiser testar a partir do consolidado
    df_resumo_das_metas = processar_tribunais(df_consolidado) 
    
    # 3. Gerar o gráfico comparativo
    if df_resumo_das_metas is not None:
        gerar_graficos(df_resumo_das_metas)
        
    print("Processamento concluído.")