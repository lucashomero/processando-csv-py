# Processamento e Análise de Metas do Poder Judiciário Brasileiro

## Descrição

Este projeto tem como objetivo processar arquivos CSV contendo dados de desempenho de diversos tribunais brasileiros. Ele consolida esses dados, calcula o desempenho de cada tribunal em relação às Metas Nacionais do Poder Judiciário (projeto para a disciplina de Programação Concorrente e Distribuída da Universidade Católica de Brasília - UCB) e gera relatórios e gráficos comparativos.

O sistema realiza um processo de ETL (Extração, Transformação e Carga) para tratar os dados dos tribunais.

## Funcionalidades

* **Consolidação de Dados**: Agrega múltiplos arquivos CSV (com prefixo `teste_*.csv`) localizados na pasta `./Dados/` em um único arquivo.
* **Cálculo de Metas**:
    * Determina o desempenho dos tribunais com base nas fórmulas especificadas para cada ramo da Justiça (Estadual, Trabalho, Federal, Militar da União, Militar Estadual, Eleitoral, Superior do Trabalho e Superior Tribunal de Justiça).
    * Utiliza colunas como `julgados_2025`, `casos_novos_2025`, `dessobrestados_2025` e `suspensos_2025` para os cálculos, conforme especificado para a Meta 1 e adaptado para as demais.
    * As colunas `sigla_tribunal` e `ramo_justica` nos arquivos de entrada são essenciais para o processamento.
* **Geração de Relatórios**:
    * Cria o arquivo `Consolidado.csv` na pasta `./saida/` com todos os dados de entrada unificados.
    * Produz o arquivo `ResumoMetas.csv` na pasta `./saida/`, detalhando o desempenho percentual de cada tribunal nas metas aplicáveis.
    * Células vazias ou não aplicáveis no `ResumoMetas.csv` são preenchidas com o valor "NA".
* **Visualização de Dados**:
    * Gera gráficos de barras (arquivos `.png`) na pasta `./saida/` para comparar o desempenho dos tribunais em metas selecionadas (ex: `Meta1`, `Meta2A`, `Meta2ANT`, `Meta4A`, `Meta6`).

## Estrutura do Projeto

```
.
├── .gitignore            # Arquivo de configuração do Git
├── README.md             # Este arquivo
├── Versao_NP.py          # Script Python para processamento sequencial
└── Versao_P.py           # Script Python para processamento paralelo
```

## Pré-requisitos

* Python 3.7 ou superior.
* Bibliotecas Pandas e Matplotlib:
    ```bash
    pip install pandas matplotlib
    ```
* **Arquivos de Dados**:
    * Os arquivos CSV de entrada devem estar no formato `teste_*.csv`.
    * Esses arquivos devem ser colocados em uma pasta chamada `Dados` na raiz do projeto.
    * A base de dados pode ser baixada através do seguinte link: [Link para baixar a base de dados](https://drive.google.com/file/d/1qL4whhMwcrR_ndJ4UAE_2KwQCtvlha5E/view?usp=sharing)
    * **Importante**: Os arquivos originais da base de dados não devem ser modificados.

## Como Executar

1.  **Configure o Ambiente**:
    * Certifique-se de que Python e as bibliotecas Pandas e Matplotlib estão instalados.
    * Crie uma pasta chamada `Dados` na raiz do projeto.
    * Baixe os arquivos CSV da base de dados utilizando o link fornecido acima e coloque-os dentro da pasta `Dados`.
2.  **Execute o Script**:
    * Abra um terminal ou prompt de comando.
    * Navegue até o diretório raiz do projeto.
    * Execute o script `Versao_NP.py`:
        ```bash
        python Versao_NP.py
        ```
3.  **Verifique os Resultados**:
    * Após a execução, a pasta `Saida` será criada (se não existir) na raiz do projeto.
    * Dentro da pasta `Saida`, você encontrará:
        * `Consolidado.csv`
        * `ResumoMetas.csv`
        * Diversos arquivos `grafico_MetaX.png`

## Detalhes das Metas

As Metas Nacionais do Poder Judiciário são diretrizes estratégicas anuais para promover eficiência, celeridade e qualidade na prestação jurisdicional. Este projeto implementa fórmulas para avaliar o desempenho dos tribunais.