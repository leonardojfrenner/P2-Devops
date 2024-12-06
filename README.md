# Challenge 3: Análise de Clusters com KMeans em Candlesticks de Ações

Este projeto utiliza técnicas de machine learning, especificamente o algoritmo KMeans, para analisar e agrupar candles de ações baseados em suas características. O objetivo é identificar padrões nos dados de ações, facilitando a visualização e análise das tendências de mercado.

---

## **Funcionalidades**

### **Leitura e Preprocessamento de Dados**
- Carrega dados de um arquivo Excel.
- Calcula componentes dos candles (tamanho do candle, corpo, sombras superior e inferior).

### **Clusterização de Candles**
- Utiliza o KMeans para agrupar candles em clusters iniciais.
- Realiza uma clusterização adicional para refinar os grupos formados.

### **Visualização de Clusters**
- Gera gráficos de candlestick para os clusters formados, permitindo a visualização dos padrões.

---

## **Pré-requisitos**

Certifique-se de ter instalado as seguintes bibliotecas:

- `pandas`
- `scikit-learn`
- `matplotlib`
- `plotly`
- `mplfinance`

Você pode instalar essas bibliotecas executando o comando:

```sh
pip install pandas scikit-learn matplotlib plotly mplfinance
```

Clone o repositório do projeto para o seu ambiente local
```sh
git clone https://github.com/leonardojfrenner/P2-Devops
```
