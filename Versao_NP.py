import pandas as pd
import matplotlib.pyplot as plt
import glob
import os
import time

# --- Configurações Iniciais ---
PASTA_DOS_CSVS = "./Dados"
PASTA_SAIDA = "./Saida"
NOME_ARQUIVO_CONSOLIDADO = "Consolidado.csv"
NOME_ARQUIVO_RESUMO_METAS = "ResumoMetas.csv"
NOME_GRAFICO_EXEMPLO = "grafico_exemplo_meta1.png"

COL_JULGADOS = 'julgados_2025'
COL_CASOS_NOVOS = 'casos_novos_2025'
COL_DESSOBRESTADOS = 'dessobrestados_2025'
COL_SUSPENSOS = 'suspensos_2025'

ALL_META_COLUMNS = [
    'tribunal', 'ramo_justica', 'Meta1', 'Meta2A', 'Meta2B', 'Meta2C', 'Meta2ANT',
    'Meta4A', 'Meta4B', 'Meta6', 'Meta7A', 'Meta7B', 'Meta8A', 'Meta8B', 'Meta8',
    'Meta10A', 'Meta10B', 'Meta10'
]

# --- 1. Leitura e Consolidação dos CSVs (Gerar Consolidado.csv) ---
def consolidar_csvs(caminho_pasta_dados, caminho_arquivo_saida_consolidado):
    """
    Lê todos os arquivos CSV de uma pasta que correspondem ao padrão 'teste_*.csv',
    consolida-os em um único DataFrame e salva em um novo arquivo CSV.
    """
    t1 = time.time()
    arquivos_csv = glob.glob(os.path.join(caminho_pasta_dados, "teste_*.csv"))
    if not arquivos_csv:
        print(f"Nenhum arquivo CSV encontrado no padrão 'teste_*.csv' na pasta: {caminho_pasta_dados}")
        return None

    lista_de_dfs = []
    for arquivo in arquivos_csv:
        try:
            df_temp = pd.read_csv(arquivo, sep=',', encoding='utf-8')
            if 'sigla_tribunal' not in df_temp.columns or 'ramo_justica' not in df_temp.columns:
                print(f"Alerta: Arquivo {arquivo} não contém 'sigla_tribunal' ou 'ramo_justica'. O processamento pode falhar.")
            lista_de_dfs.append(df_temp)
            print(f"Arquivo {arquivo} lido com sucesso.")
        except Exception as e:
            print(f"Erro ao ler o arquivo {arquivo}: {e}")

    if not lista_de_dfs:
        print("Nenhum DataFrame para concatenar.")
        return None

    df_consolidado = pd.concat(lista_de_dfs, ignore_index=True)
    try:
        df_consolidado.to_csv(caminho_arquivo_saida_consolidado, index=False, sep=',', encoding='utf-8')
        print(f"Arquivo consolidado '{caminho_arquivo_saida_consolidado}' gerado com sucesso com {len(df_consolidado)} linhas.")
    except Exception as e:
        print(f"Erro ao salvar o arquivo consolidado '{caminho_arquivo_saida_consolidado}': {e}")
    t2 = time.time()
    print(f"Tempo de execução (consolidar csvs): {t2 - t1} segundos")
    return df_consolidado

# --- 2. Funções Auxiliares para Cálculo de Metas ---
def calcular_meta_tipo_1(df_tribunal):
    """
    Calcula metas com a fórmula: (Σ julgados / (Σ casos_novos + Σ dessobrestados - Σ suspensos)) * 100
    Aplicável à Meta 1 de todos os tribunais.
    """
    soma_julgados = df_tribunal[COL_JULGADOS].sum()
    soma_casos_novos = df_tribunal[COL_CASOS_NOVOS].sum()
    soma_dessobrestados = df_tribunal[COL_DESSOBRESTADOS].sum()
    soma_suspensos = df_tribunal[COL_SUSPENSOS].sum()

    denominador = soma_casos_novos + soma_dessobrestados - soma_suspensos
    if denominador == 0:
        return "NA"
    return (soma_julgados / denominador) * 100

def calcular_meta_generica(df_tribunal, multiplicador):
    """
    Calcula metas com a fórmula: (Σ julgados / (Σ casos_novos - Σ suspensos)) * multiplicador
    Onde 'casos_novos' representa o ΣdismX das fórmulas.
    O multiplicador varia (ex: 100, ou 1000/fator_P).
    """
    soma_julgados = df_tribunal[COL_JULGADOS].sum()
    soma_distribuidos = df_tribunal[COL_CASOS_NOVOS].sum()
    soma_suspensos = df_tribunal[COL_SUSPENSOS].sum()

    denominador = soma_distribuidos - soma_suspensos
    if denominador == 0:
        return "NA"
    return (soma_julgados / denominador) * multiplicador

# --- 3. Funções de Cálculo de Metas por Ramo da Justiça ---
def calcular_metas_justica_estadual(df_tribunal):
    """Calcula todas as metas aplicáveis à Justiça Estadual."""
    resultados = {}
    resultados['Meta1'] = calcular_meta_tipo_1(df_tribunal)
    resultados['Meta2A'] = calcular_meta_generica(df_tribunal, multiplicador=(1000/8))
    resultados['Meta2B'] = calcular_meta_generica(df_tribunal, multiplicador=(1000/9))
    resultados['Meta2C'] = calcular_meta_generica(df_tribunal, multiplicador=(1000/9.5))
    resultados['Meta2ANT'] = calcular_meta_generica(df_tribunal, multiplicador=100)
    resultados['Meta4A'] = calcular_meta_generica(df_tribunal, multiplicador=(1000/6.5))
    resultados['Meta4B'] = calcular_meta_generica(df_tribunal, multiplicador=100)
    resultados['Meta6'] = calcular_meta_generica(df_tribunal, multiplicador=100)
    resultados['Meta7A'] = calcular_meta_generica(df_tribunal, multiplicador=(1000/5))
    resultados['Meta7B'] = calcular_meta_generica(df_tribunal, multiplicador=(1000/5))
    resultados['Meta8A'] = calcular_meta_generica(df_tribunal, multiplicador=(1000/7.5))
    resultados['Meta8B'] = calcular_meta_generica(df_tribunal, multiplicador=(1000/9))
    resultados['Meta10A'] = calcular_meta_generica(df_tribunal, multiplicador=(1000/9))
    resultados['Meta10B'] = calcular_meta_generica(df_tribunal, multiplicador=(1000/10))
    return resultados

def calcular_metas_justica_trabalho(df_tribunal):
    """Calcula todas as metas aplicáveis à Justiça do Trabalho."""
    resultados = {}
    resultados['Meta1'] = calcular_meta_tipo_1(df_tribunal)
    resultados['Meta2A'] = calcular_meta_generica(df_tribunal, multiplicador=(1000/9.4))
    resultados['Meta2ANT'] = calcular_meta_generica(df_tribunal, multiplicador=100)
    resultados['Meta4A'] = calcular_meta_generica(df_tribunal, multiplicador=(1000/7))
    resultados['Meta4B'] = calcular_meta_generica(df_tribunal, multiplicador=100)
    return resultados

def calcular_metas_justica_federal(df_tribunal):
    """Calcula todas as metas aplicáveis à Justiça Federal."""
    resultados = {}
    resultados['Meta1'] = calcular_meta_tipo_1(df_tribunal)
    resultados['Meta2A'] = calcular_meta_generica(df_tribunal, multiplicador=(1000/8.5))
    resultados['Meta2B'] = calcular_meta_generica(df_tribunal, multiplicador=100)
    resultados['Meta2ANT'] = calcular_meta_generica(df_tribunal, multiplicador=100)
    resultados['Meta4A'] = calcular_meta_generica(df_tribunal, multiplicador=(1000/7))
    resultados['Meta4B'] = calcular_meta_generica(df_tribunal, multiplicador=100)
    resultados['Meta6'] = calcular_meta_generica(df_tribunal, multiplicador=(1000/3.5))
    resultados['Meta7A'] = calcular_meta_generica(df_tribunal, multiplicador=(1000/3.5))
    resultados['Meta7B'] = calcular_meta_generica(df_tribunal, multiplicador=(1000/3.5))
    resultados['Meta8A'] = calcular_meta_generica(df_tribunal, multiplicador=(1000/7.5))
    resultados['Meta8B'] = calcular_meta_generica(df_tribunal, multiplicador=(1000/9))
    resultados['Meta10A'] = calcular_meta_generica(df_tribunal, multiplicador=100)
    return resultados

def calcular_metas_justica_militar_uniao(df_tribunal):
    """Calcula todas as metas aplicáveis à Justiça Militar da União."""
    resultados = {}
    resultados['Meta1'] = calcular_meta_tipo_1(df_tribunal)
    resultados['Meta2A'] = calcular_meta_generica(df_tribunal, multiplicador=(1000/9.5))
    resultados['Meta2B'] = calcular_meta_generica(df_tribunal, multiplicador=(1000/9.9))
    resultados['Meta2ANT'] = calcular_meta_generica(df_tribunal, multiplicador=100)
    resultados['Meta4A'] = calcular_meta_generica(df_tribunal, multiplicador=(1000/9.5))
    resultados['Meta4B'] = calcular_meta_generica(df_tribunal, multiplicador=(1000/9.9))
    return resultados

def calcular_metas_justica_militar_estadual(df_tribunal):
    """Calcula todas as metas aplicáveis à Justiça Militar Estadual."""
    resultados = {}
    resultados['Meta1'] = calcular_meta_tipo_1(df_tribunal)
    resultados['Meta2A'] = calcular_meta_generica(df_tribunal, multiplicador=(1000/9))
    resultados['Meta2B'] = calcular_meta_generica(df_tribunal, multiplicador=(1000/9.5))
    resultados['Meta2ANT'] = calcular_meta_generica(df_tribunal, multiplicador=100)
    resultados['Meta4A'] = calcular_meta_generica(df_tribunal, multiplicador=(1000/9.5))
    resultados['Meta4B'] = calcular_meta_generica(df_tribunal, multiplicador=(1000/9.9))
    return resultados

def calcular_metas_tribunal_superior_eleitoral(df_tribunal):
    """Calcula todas as metas aplicáveis ao Tribunal Superior Eleitoral."""
    resultados = {}
    resultados['Meta1'] = calcular_meta_tipo_1(df_tribunal)
    resultados['Meta2A'] = calcular_meta_generica(df_tribunal, multiplicador=(1000/7))
    resultados['Meta2B'] = calcular_meta_generica(df_tribunal, multiplicador=(1000/9.9))
    resultados['Meta2ANT'] = calcular_meta_generica(df_tribunal, multiplicador=100)
    resultados['Meta4A'] = calcular_meta_generica(df_tribunal, multiplicador=(1000/9))
    resultados['Meta4B'] = calcular_meta_generica(df_tribunal, multiplicador=(1000/5))
    return resultados

def calcular_metas_tribunal_superior_trabalho(df_tribunal):
    """Calcula todas as metas aplicáveis ao Tribunal Superior do Trabalho."""
    resultados = {}
    resultados['Meta1'] = calcular_meta_tipo_1(df_tribunal)
    resultados['Meta2A'] = calcular_meta_generica(df_tribunal, multiplicador=(1000/9.5))
    resultados['Meta2B'] = calcular_meta_generica(df_tribunal, multiplicador=(1000/9.9))
    resultados['Meta2ANT'] = calcular_meta_generica(df_tribunal, multiplicador=100)
    resultados['Meta4A'] = calcular_meta_generica(df_tribunal, multiplicador=(1000/7))
    resultados['Meta4B'] = calcular_meta_generica(df_tribunal, multiplicador=100)
    return resultados

def calcular_metas_superior_tribunal_justica(df_tribunal):
    """Calcula todas as metas aplicáveis ao Superior Tribunal de Justiça."""
    resultados = {}
    resultados['Meta1'] = calcular_meta_tipo_1(df_tribunal)
    resultados['Meta2ANT'] = calcular_meta_generica(df_tribunal, multiplicador=100)
    resultados['Meta4A'] = calcular_meta_generica(df_tribunal, multiplicador=(1000/9))
    resultados['Meta4B'] = calcular_meta_generica(df_tribunal, multiplicador=100)
    resultados['Meta6'] = calcular_meta_generica(df_tribunal, multiplicador=(1000/7.5))
    resultados['Meta7A'] = calcular_meta_generica(df_tribunal, multiplicador=(1000/7.5))
    resultados['Meta7B'] = calcular_meta_generica(df_tribunal, multiplicador=(1000/7.5))
    resultados['Meta8'] = calcular_meta_generica(df_tribunal, multiplicador=(1000/10))
    resultados['Meta10'] = calcular_meta_generica(df_tribunal, multiplicador=(1000/10))
    return resultados

# --- 4. Processamento Principal dos Tribunais ---
def processar_tribunais(df_dados_consolidados, caminho_arquivo_saida_resumo):
    """
    Processa os dados consolidados para calcular as metas de cada tribunal,
    chama a função de cálculo apropriada baseada no 'ramo_justica' e
    salva os resultados em um arquivo CSV.
    """
    t1 = time.time()
    if df_dados_consolidados is None or df_dados_consolidados.empty:
        print("DataFrame consolidado está vazio. Não é possível processar tribunais.")
        return None

    if 'sigla_tribunal' not in df_dados_consolidados.columns:
        print("Erro: Coluna 'sigla_tribunal' não encontrada no DataFrame consolidado.")
        return None
    if 'ramo_justica' not in df_dados_consolidados.columns:
        print("Erro: Coluna 'ramo_justica' não encontrada no DataFrame consolidado.")
        return None

    resultados_gerais = []

    for tribunal_sigla, df_tribunal_especifico in df_dados_consolidados.groupby('sigla_tribunal'):
        print(f"Processando tribunal: {tribunal_sigla}")
        ramo = df_tribunal_especifico['ramo_justica'].iloc[0]
        desempenho_tribunal = {'tribunal': tribunal_sigla, 'ramo_justica': ramo}
        calculated_metas = {}
        df_copy = df_tribunal_especifico.copy()

        if ramo == "Justiça Estadual":
            calculated_metas = calcular_metas_justica_estadual(df_copy)
        elif ramo == "Justiça do Trabalho":
            calculated_metas = calcular_metas_justica_trabalho(df_copy)
        elif ramo == "Justiça Federal":
            calculated_metas = calcular_metas_justica_federal(df_copy)
        elif ramo == "Justiça Militar da União":
            calculated_metas = calcular_metas_justica_militar_uniao(df_copy)
        elif ramo == "Justiça Militar Estadual":
            calculated_metas = calcular_metas_justica_militar_estadual(df_copy)
        elif ramo == "Tribunal Superior Eleitoral":
             calculated_metas = calcular_metas_tribunal_superior_eleitoral(df_copy)
        elif ramo == "Tribunal Superior do Trabalho":
             calculated_metas = calcular_metas_tribunal_superior_trabalho(df_copy)
        elif ramo == "Superior Tribunal de Justiça":
             calculated_metas = calcular_metas_superior_tribunal_justica(df_copy)
        else:
            print(f"Alerta: Ramo de justiça '{ramo}' para o tribunal '{tribunal_sigla}' não possui função de cálculo de metas definida.")
            desempenho_tribunal['Meta1'] = calcular_meta_tipo_1(df_copy)

        desempenho_tribunal.update(calculated_metas)
        resultados_gerais.append(desempenho_tribunal)

    df_resumo_metas = pd.DataFrame(resultados_gerais)
    cols_ordenadas = ['tribunal', 'ramo_justica'] + [m for m in ALL_META_COLUMNS if m not in ['tribunal', 'ramo_justica']]
    df_resumo_metas = df_resumo_metas.reindex(columns=cols_ordenadas).fillna("NA")

    try:
        df_resumo_metas.to_csv(caminho_arquivo_saida_resumo, index=False, sep=',', encoding='utf-8')
        print(f"Arquivo de resumo de metas '{caminho_arquivo_saida_resumo}' gerado com sucesso.")
    except Exception as e:
        print(f"Erro ao salvar o arquivo de resumo de metas '{caminho_arquivo_saida_resumo}': {e}")
    t2 = time.time()
    print(f"Tempo de execução (processar tribunais): {t2 - t1:.2f} segundos")
    return df_resumo_metas

# --- 5. Geração de Gráficos ---
def gerar_graficos(df_resumo, pasta_saida_graficos):
    """
    Gera gráficos de barras comparativos para um conjunto selecionado de metas.
    """
    t1 = time.time()
    if df_resumo is None or df_resumo.empty:
        print("DataFrame de resumo vazio ou nulo. Não é possível gerar gráficos.")
        return

    if not os.path.exists(pasta_saida_graficos):
        try:
            os.makedirs(pasta_saida_graficos, exist_ok=True)
            print(f"Pasta para gráficos '{pasta_saida_graficos}' criada.")
        except OSError as e:
            print(f"Erro ao criar diretório para gráficos '{pasta_saida_graficos}': {e}. Gráficos não serão salvos.")
            return

    metas_para_plotar = ['Meta1', 'Meta2A', 'Meta2ANT', 'Meta4A', 'Meta6']
    num_tribunais_top = 15

    for meta_nome in metas_para_plotar:
        if meta_nome not in df_resumo.columns:
            print(f"Meta '{meta_nome}' não encontrada no DataFrame de resumo. Pulando gráfico.")
            continue

        print(f"Gerando gráfico para {meta_nome}...")
        df_para_plot = df_resumo[['tribunal', meta_nome]].copy()
        df_para_plot[meta_nome] = pd.to_numeric(df_para_plot[meta_nome], errors='coerce')
        df_para_plot = df_para_plot.dropna(subset=[meta_nome])
        df_para_plot = df_para_plot.sort_values(by=meta_nome, ascending=False).head(num_tribunais_top)

        if not df_para_plot.empty:
            plt.figure(figsize=(14, 8))
            bars = plt.bar(df_para_plot['tribunal'], df_para_plot[meta_nome])
            plt.title(f'Desempenho - {meta_nome} (Top {num_tribunais_top} Tribunais)', fontsize=16)
            plt.ylabel(f'Valor da {meta_nome}', fontsize=12)
            plt.xlabel('Tribunal', fontsize=12)
            plt.xticks(rotation=45, ha="right", fontsize=10)
            plt.yticks(fontsize=10)
            plt.grid(axis='y', linestyle='--')

            for bar in bars:
                yval = bar.get_height()
                plt.text(bar.get_x() + bar.get_width()/2.0, yval + 0.01 * df_para_plot[meta_nome].max(), # Posição do texto
                         f'{yval:.2f}', ha='center', va='bottom', fontsize=9)

            plt.tight_layout()
            caminho_arquivo_grafico = os.path.join(pasta_saida_graficos, f"grafico_{meta_nome}.png")
            try:
                plt.savefig(caminho_arquivo_grafico)
                print(f"Gráfico '{caminho_arquivo_grafico}' salvo com sucesso.")
            except Exception as e:
                print(f"Erro ao salvar o gráfico '{caminho_arquivo_grafico}': {e}")
            plt.close()
        else:
            print(f"Não há dados válidos para '{meta_nome}' para gerar o gráfico.")
    print("Geração de gráficos concluída.")
    t2 = time.time()
    print(f"Tempo de execução (geração de gráficos): {t2 - t1:.2f} segundos.")

# --- Função Principal (Main) ---
if __name__ == "__main__":
    t1 = time.time()
    print("Iniciando processamento (Versao_NP.py)...")

    try:
        os.makedirs(PASTA_SAIDA, exist_ok=True)
        print(f"Pasta de saída '{PASTA_SAIDA}' verificada/criada.")
    except OSError as e:
        print(f"Erro ao criar diretório de saída '{PASTA_SAIDA}': {e}")

    caminho_consolidado = os.path.join(PASTA_SAIDA, NOME_ARQUIVO_CONSOLIDADO)
    caminho_resumo_metas = os.path.join(PASTA_SAIDA, NOME_ARQUIVO_RESUMO_METAS)

    df_consolidado = consolidar_csvs(PASTA_DOS_CSVS, caminho_consolidado)
    df_resumo_das_metas = processar_tribunais(df_consolidado, caminho_resumo_metas)

    if df_resumo_das_metas is not None:
        gerar_graficos(df_resumo_das_metas, PASTA_SAIDA)
    else:
        print("Não foi possível gerar gráficos pois o resumo das metas não foi criado.")

    print("Processamento concluído.")
    t2 = time.time()
    print(f"Tempo total de execução: {t2 - t1:.2f} segundos.") # 150.90 segundos