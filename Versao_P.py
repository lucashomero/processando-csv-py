import pandas as pd
import matplotlib.pyplot as plt
import glob
import os
import time
from concurrent.futures import ProcessPoolExecutor, as_completed

# --- Configurações Iniciais ---
PASTA_DOS_CSVS = "./Dados"
PASTA_SAIDA = "./Saida_P" # Saída em pasta separada para a versão paralela
NOME_ARQUIVO_CONSOLIDADO = "Consolidado.csv"
NOME_ARQUIVO_RESUMO_METAS = "ResumoMetas_P.csv"

# --- Constantes de Colunas (do seu script original) ---
COL_JULGADOS = 'julgados_2025'
COL_CASOS_NOVOS = 'casos_novos_2025'
COL_DESSOBRESTADOS = 'dessobrestados_2025'
COL_SUSPENSOS = 'suspensos_2025'

ALL_META_COLUMNS = [
    'tribunal', 'ramo_justica', 'Meta1', 'Meta2A', 'Meta2B', 'Meta2C', 'Meta2ANT',
    'Meta4A', 'Meta4B', 'Meta6', 'Meta7A', 'Meta7B', 'Meta8A', 'Meta8B', 'Meta8',
    'Meta10A', 'Meta10B', 'Meta10'
]

# --- 1. Leitura e Consolidação dos CSVs ---
def consolidar_csvs(caminho_pasta_dados, caminho_arquivo_saida_consolidado):
    """Lê todos os arquivos CSV de uma pasta, consolida-os e salva em um novo arquivo."""
    t1 = time.time()
    print("Iniciando consolidação dos arquivos CSV...")
    arquivos_csv = glob.glob(os.path.join(caminho_pasta_dados, "teste_*.csv"))
    if not arquivos_csv:
        print(f"Nenhum arquivo CSV encontrado no padrão 'teste_*.csv' na pasta: {caminho_pasta_dados}")
        return None

    try:
        lista_de_dfs = [pd.read_csv(arquivo, sep=',', encoding='utf-8') for arquivo in arquivos_csv]
    except Exception as e:
        print(f"Erro ao ler um dos arquivos CSV: {e}")
        return None

    if not lista_de_dfs:
        print("Nenhum DataFrame para concatenar.")
        return None

    df_consolidado = pd.concat(lista_de_dfs, ignore_index=True)
    df_consolidado.to_csv(caminho_arquivo_saida_consolidado, index=False, sep=',', encoding='utf-8')
    
    t2 = time.time()
    print(f"Arquivo consolidado gerado em {t2 - t1:.2f} segundos com {len(df_consolidado)} linhas.")
    return df_consolidado

# --- 2. Funções Auxiliares para Cálculo de Metas ---
def calcular_meta_tipo_1(df_tribunal):
    """Calcula metas com a fórmula: (Σ julgados / (Σ casos_novos + Σ dessobrestados - Σ suspensos)) * 100"""
    soma_julgados = df_tribunal[COL_JULGADOS].sum()
    soma_casos_novos = df_tribunal[COL_CASOS_NOVOS].sum()
    soma_dessobrestados = df_tribunal[COL_DESSOBRESTADOS].sum()
    soma_suspensos = df_tribunal[COL_SUSPENSOS].sum()
    denominador = soma_casos_novos + soma_dessobrestados - soma_suspensos
    return (soma_julgados / denominador) * 100 if denominador != 0 else "NA"

def calcular_meta_generica(df_tribunal, multiplicador):
    """Calcula metas com a fórmula: (Σ julgados / (Σ casos_novos - Σ suspensos)) * multiplicador"""
    soma_julgados = df_tribunal[COL_JULGADOS].sum()
    soma_distribuidos = df_tribunal[COL_CASOS_NOVOS].sum()
    soma_suspensos = df_tribunal[COL_SUSPENSOS].sum()
    denominador = soma_distribuidos - soma_suspensos
    return (soma_julgados / denominador) * multiplicador if denominador != 0 else "NA"

# --- 3. Funções de Cálculo de Metas por Ramo da Justiça (Completas) ---
def calcular_metas_justica_estadual(df_tribunal):
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
    resultados = {}
    resultados['Meta1'] = calcular_meta_tipo_1(df_tribunal)
    resultados['Meta2A'] = calcular_meta_generica(df_tribunal, multiplicador=(1000/9.4))
    resultados['Meta2ANT'] = calcular_meta_generica(df_tribunal, multiplicador=100)
    resultados['Meta4A'] = calcular_meta_generica(df_tribunal, multiplicador=(1000/7))
    resultados['Meta4B'] = calcular_meta_generica(df_tribunal, multiplicador=100)
    return resultados

def calcular_metas_justica_federal(df_tribunal):
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
    resultados = {}
    resultados['Meta1'] = calcular_meta_tipo_1(df_tribunal)
    resultados['Meta2A'] = calcular_meta_generica(df_tribunal, multiplicador=(1000/9.5))
    resultados['Meta2B'] = calcular_meta_generica(df_tribunal, multiplicador=(1000/9.9))
    resultados['Meta2ANT'] = calcular_meta_generica(df_tribunal, multiplicador=100)
    resultados['Meta4A'] = calcular_meta_generica(df_tribunal, multiplicador=(1000/9.5))
    resultados['Meta4B'] = calcular_meta_generica(df_tribunal, multiplicador=(1000/9.9))
    return resultados

def calcular_metas_justica_militar_estadual(df_tribunal):
    resultados = {}
    resultados['Meta1'] = calcular_meta_tipo_1(df_tribunal)
    resultados['Meta2A'] = calcular_meta_generica(df_tribunal, multiplicador=(1000/9))
    resultados['Meta2B'] = calcular_meta_generica(df_tribunal, multiplicador=(1000/9.5))
    resultados['Meta2ANT'] = calcular_meta_generica(df_tribunal, multiplicador=100)
    resultados['Meta4A'] = calcular_meta_generica(df_tribunal, multiplicador=(1000/9.5))
    resultados['Meta4B'] = calcular_meta_generica(df_tribunal, multiplicador=(1000/9.9))
    return resultados

def calcular_metas_tribunal_superior_eleitoral(df_tribunal):
    resultados = {}
    resultados['Meta1'] = calcular_meta_tipo_1(df_tribunal)
    resultados['Meta2A'] = calcular_meta_generica(df_tribunal, multiplicador=(1000/7))
    resultados['Meta2B'] = calcular_meta_generica(df_tribunal, multiplicador=(1000/9.9))
    resultados['Meta2ANT'] = calcular_meta_generica(df_tribunal, multiplicador=100)
    resultados['Meta4A'] = calcular_meta_generica(df_tribunal, multiplicador=(1000/9))
    resultados['Meta4B'] = calcular_meta_generica(df_tribunal, multiplicador=(1000/5))
    return resultados

def calcular_metas_tribunal_superior_trabalho(df_tribunal):
    resultados = {}
    resultados['Meta1'] = calcular_meta_tipo_1(df_tribunal)
    resultados['Meta2A'] = calcular_meta_generica(df_tribunal, multiplicador=(1000/9.5))
    resultados['Meta2B'] = calcular_meta_generica(df_tribunal, multiplicador=(1000/9.9))
    resultados['Meta2ANT'] = calcular_meta_generica(df_tribunal, multiplicador=100)
    resultados['Meta4A'] = calcular_meta_generica(df_tribunal, multiplicador=(1000/7))
    resultados['Meta4B'] = calcular_meta_generica(df_tribunal, multiplicador=100)
    return resultados

def calcular_metas_superior_tribunal_justica(df_tribunal):
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

# --- 4. FUNÇÃO TRABALHADORA PARA PARALELIZAÇÃO ---
def worker_processar_tribunal(args):
    """
    Função "trabalhadora" que processa os dados de UM tribunal.
    Será executada em um processo separado para cada tribunal.
    """
    tribunal_sigla, df_tribunal_especifico = args
    ramo = df_tribunal_especifico['ramo_justica'].iloc[0]
    desempenho_tribunal = {'tribunal': tribunal_sigla, 'ramo_justica': ramo}
    
    mapeamento_funcoes = {
        "Justiça Estadual": calcular_metas_justica_estadual,
        "Justiça do Trabalho": calcular_metas_justica_trabalho,
        "Justiça Federal": calcular_metas_justica_federal,
        "Justiça Militar da União": calcular_metas_justica_militar_uniao,
        "Justiça Militar Estadual": calcular_metas_justica_militar_estadual,
        "Tribunal Superior Eleitoral": calcular_metas_tribunal_superior_eleitoral,
        "Tribunal Superior do Trabalho": calcular_metas_tribunal_superior_trabalho,
        "Superior Tribunal de Justiça": calcular_metas_superior_tribunal_justica
    }

    if ramo in mapeamento_funcoes:
        calculated_metas = mapeamento_funcoes[ramo](df_tribunal_especifico)
        desempenho_tribunal.update(calculated_metas)
    else:
        print(f"Alerta: Ramo '{ramo}' do tribunal '{tribunal_sigla}' não possui mapeamento de função.")

    return desempenho_tribunal

# --- 5. Processamento Principal dos Tribunais (Versão Paralela) ---
def processar_tribunais_paralelo(df_dados_consolidados, caminho_arquivo_saida_resumo):
    """Processa os dados em paralelo, distribuindo o cálculo de cada tribunal."""
    t1 = time.time()
    if df_dados_consolidados is None or df_dados_consolidados.empty:
        print("DataFrame consolidado vazio. Não é possível processar.")
        return None

    dados_agrupados_por_tribunal = list(df_dados_consolidados.groupby('sigla_tribunal'))
    resultados_gerais = []
    
    with ProcessPoolExecutor() as executor:
        print(f"Iniciando processamento paralelo com {executor._max_workers} processos...")
        
        futures = {executor.submit(worker_processar_tribunal, args): args[0] for args in dados_agrupados_por_tribunal}
        
        for future in as_completed(futures):
            tribunal_nome = futures[future]
            try:
                resultado = future.result()
                resultados_gerais.append(resultado)
                print(f"Tribunal '{tribunal_nome}' processado com sucesso.")
            except Exception as e:
                print(f"Erro ao processar o tribunal '{tribunal_nome}': {e}")

    df_resumo_metas = pd.DataFrame(resultados_gerais)
    cols_ordenadas = [col for col in ALL_META_COLUMNS if col in df_resumo_metas.columns]
    df_resumo_metas = df_resumo_metas.reindex(columns=cols_ordenadas).fillna("NA")
    
    df_resumo_metas.to_csv(caminho_arquivo_saida_resumo, index=False, sep=',', encoding='utf-8')
    t2 = time.time()
    print(f"\nArquivo de resumo de metas '{caminho_arquivo_saida_resumo}' gerado.")
    print(f"Tempo de execução (processar tribunais PARALELO): {t2 - t1:.2f} segundos")
    return df_resumo_metas

# --- 6. Geração de Gráficos ---
def gerar_graficos(df_resumo, pasta_saida_graficos):
    """Gera gráficos de barras comparativos para um conjunto de metas."""
    t1 = time.time()
    if df_resumo is None or df_resumo.empty:
        print("DataFrame de resumo vazio. Gráficos não serão gerados.")
        return

    os.makedirs(pasta_saida_graficos, exist_ok=True)
    
    metas_para_plotar = ['Meta1', 'Meta2A', 'Meta4A', 'Meta6']
    num_tribunais_top = 15

    for meta_nome in metas_para_plotar:
        if meta_nome not in df_resumo.columns:
            continue

        print(f"Gerando gráfico para {meta_nome}...")
        df_plot = df_resumo[['tribunal', meta_nome]].copy()
        df_plot[meta_nome] = pd.to_numeric(df_plot[meta_nome], errors='coerce')
        df_plot = df_plot.dropna(subset=[meta_nome]).sort_values(by=meta_nome, ascending=False).head(num_tribunais_top)

        if not df_plot.empty:
            plt.figure(figsize=(14, 8))
            bars = plt.bar(df_plot['tribunal'], df_plot[meta_nome])
            plt.title(f'Desempenho - {meta_nome} (Top {num_tribunais_top} Tribunais)', fontsize=16)
            plt.ylabel(f'Valor da {meta_nome}', fontsize=12)
            plt.xlabel('Tribunal', fontsize=12)
            plt.xticks(rotation=45, ha="right")
            plt.grid(axis='y', linestyle='--')
            
            for bar in bars:
                yval = bar.get_height()
                plt.text(bar.get_x() + bar.get_width()/2.0, yval, f'{yval:.2f}', ha='center', va='bottom')
            
            plt.tight_layout()
            caminho_grafico = os.path.join(pasta_saida_graficos, f"grafico_{meta_nome}.png")
            plt.savefig(caminho_grafico)
            plt.close()
        else:
            print(f"Sem dados válidos para gerar gráfico da {meta_nome}.")
    t2 = time.time()
    print(f"Geração de gráficos concluída em {t2 - t1:.2f} segundos.")


# --- Função Principal (Main) ---
if __name__ == "__main__":
    t_inicio_total = time.time()
    print("--- INICIANDO PROCESSAMENTO PARALELO (Versao_P.py) ---")

    os.makedirs(PASTA_SAIDA, exist_ok=True)
    caminho_consolidado = os.path.join(PASTA_SAIDA, NOME_ARQUIVO_CONSOLIDADO)
    caminho_resumo_metas = os.path.join(PASTA_SAIDA, NOME_ARQUIVO_RESUMO_METAS)

    # Etapa 1: Consolidação
    df_consolidado = consolidar_csvs(PASTA_DOS_CSVS, caminho_consolidado)
    
    if df_consolidado is not None:
        # Etapa 2: Processamento Paralelo
        df_resumo_das_metas = processar_tribunais_paralelo(df_consolidado, caminho_resumo_metas)

        if df_resumo_das_metas is not None:
            # Etapa 3: Geração de Gráficos
            gerar_graficos(df_resumo_das_metas, PASTA_SAIDA)
    else:
        print("Processamento interrompido pois a consolidação falhou.")
    
    t_fim_total = time.time()
    print("\n--- PROCESSAMENTO PARALELO CONCLUÍDO ---")
    print(f"Tempo total de execução: {t_fim_total - t_inicio_total:.2f} segundos.")
