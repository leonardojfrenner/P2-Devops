from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score
from sklearn.preprocessing import StandardScaler
import plotly.graph_objects as go

# Função para carregar o arquivo e processar as datas
def carregar_arquivo(**kwargs):
    file_path = "/opt/airflow/p2-ci-cd/Challenge 3_NFLX.xlsx"
    try:
        # Carregar o arquivo Excel
        df = pd.read_excel(file_path, sheet_name="NFLX")
        
        # Limitar a 500 linhas
        df = df.head(500)
        
        # Convertendo a coluna 'Date' para string
        df["Date"] = pd.to_datetime(df["Date"]).dt.strftime('%Y-%m-%d %H:%M:%S')
        
        # Se houver outras colunas de data, converte-as para string
        for col in df.select_dtypes(include=['datetime']):
            df[col] = df[col].dt.strftime('%Y-%m-%d %H:%M:%S')
        
        # Envia o dataframe convertido como XCom (armazenando como dict)
        kwargs['ti'].xcom_push(key='dataframe', value=df.to_dict('records'))
    except Exception as e:
        raise ValueError(f"Erro ao carregar arquivo: {e}")

# Função para calcular os componentes do candle
def calcular_componentes(row):
    candle_size = row["High"] - row["Low"]
    body = abs(row["Close"] - row["Open"])
    return {
        "candle_size": round(candle_size, 2),
        "body": round(body, 2),
        "body_proportion": round((body / candle_size) * 100, 2),
        "shadow": round((candle_size - body), 2),
        "upper_shadow": round((row["High"] - max(row["Open"], row["Close"])), 2),
        "lower_shadow": round((min(row["Open"], row["Close"]) - row["Low"]), 2),
    }

# Função para separar os candles (verde ou vermelho) e calcular componentes
def separar_candles(**kwargs):
    # Pega o dataframe passado pela task anterior
    data = kwargs['ti'].xcom_pull(task_ids='carregar_arquivo', key='dataframe')
    df = pd.DataFrame(data)
    
    # Convertendo as colunas de data para string
    df['Date'] = pd.to_datetime(df['Date']).dt.strftime('%Y-%m-%d %H:%M:%S')
    
    # Definindo a coluna 'candle' (1 para vela verde e 0 para vela vermelha)
    df['candle'] = (df['Close'] > df['Open']).astype(int)
    
    # Calculando os componentes dos candles
    componentes = df.apply(calcular_componentes, axis=1, result_type="expand")
    
    # Concatenando os componentes calculados ao DataFrame
    df = pd.concat([df, componentes], axis=1)
    
    # Enviando o DataFrame com os resultados para o XCom (convertendo para dict)
    kwargs['ti'].xcom_push(key='dataframe', value=df.to_dict('records'))

# Função para clusterizar os candles
def clusterizar_candles(**kwargs):
    # Pega o dataframe com os componentes calculados
    data = kwargs['ti'].xcom_pull(task_ids='separar_candles', key='dataframe')
    df = pd.DataFrame(data)
    
    # Selecionando as features para o clustering
    features = df[['candle', 'body']]
    
    # Escalonando as features
    scaler = StandardScaler()
    features_scaled = scaler.fit_transform(features)
    
    # Aplicando o KMeans para clusterização
    kmeans = KMeans(n_clusters=4, random_state=0)
    df['cluster'] = kmeans.fit_predict(features_scaled)
    
    # Calculando o Silhouette Score
    silhouette_avg = silhouette_score(features_scaled, df['cluster'])
    print(f'Silhouette Score: {silhouette_avg:.3f}')
    
    # Enviando o DataFrame com os clusters para o XCom
    kwargs['ti'].xcom_push(key='dataframe', value=df.to_dict('records'))

# Função para plotar os clusters
def plot_clusters(**kwargs):
    # Pega o dataframe com os clusters
    data = kwargs['ti'].xcom_pull(task_ids='clusterizar_candles', key='dataframe')
    df = pd.DataFrame(data)
    
    # Convertendo a coluna 'Date' de volta para datetime
    df['Date'] = pd.to_datetime(df['Date'])
    
    # Adicionando o texto para o hover
    df['hover_text'] = df.apply(lambda row: f"Open: {row['Open']}<br>High: {row['High']}<br>Low: {row['Low']}<br>Close: {row['Close']}", axis=1)
    
    # Definindo o intervalo de datas para o gráfico
    start_date = "2020-09-01"
    end_date = "2021-03-31"
    df_filtered = df[(df['Date'] >= start_date) & (df['Date'] <= end_date)]

    # Agrupando os dados por cluster
    sub_clusters = df_filtered['cluster'].unique()
    
    # Criando o gráfico de candlestick
    fig = go.Figure()
    for sub_cluster in sub_clusters:
        df_sub_cluster = df_filtered[df_filtered['cluster'] == sub_cluster]
        fig.add_trace(go.Candlestick(
            x=df_sub_cluster['Date'],
            open=df_sub_cluster['Open'],
            high=df_sub_cluster['High'],
            low=df_sub_cluster['Low'],
            close=df_sub_cluster['Close'],
            name=f'Sub-cluster {sub_cluster}',
            hovertext=df_sub_cluster['hover_text'],
            hoverinfo='text',
            increasing_line_color="green",
            decreasing_line_color="red"
        ))

    # Atualizando o layout do gráfico
    fig.update_layout(
        title='Candlestick Chart - Clusters (Set 2020 - Mar 2021)',
        xaxis_title='Date',
        yaxis_title='Price',
        xaxis_rangeslider_visible=False,
    )
    
    # Salvando o gráfico em um arquivo (para Airflow)
    fig.write_html("/opt/airflow/output/candlestick_chart.html")

# Definindo os argumentos padrão do DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,  # Não tenta novamente em caso de erro
    "retry_delay": timedelta(minutes=5),
}

# Criando o DAG
with DAG(
    dag_id="candlestick_analysis",
    default_args=default_args,
    description="Pipeline para análise de candlesticks",
    schedule_interval="0 0 * * *",  # Execução diária
    start_date=datetime(2024, 11, 20),
    catchup=False,
    tags=["candlestick", "analysis"],
) as dag:
    
    # Definindo as tasks do DAG
    task_carregar_arquivo = PythonOperator(
        task_id="carregar_arquivo",
        python_callable=carregar_arquivo,
        provide_context=True,
    )

    task_separar_candles = PythonOperator(
        task_id="separar_candles",
        python_callable=separar_candles,
        provide_context=True,
    )

    task_clusterizar_candles = PythonOperator(
        task_id="clusterizar_candles",
        python_callable=clusterizar_candles,
        provide_context=True,
    )

    task_plot_clusters = PythonOperator(
        task_id="plot_clusters",
        python_callable=plot_clusters,
        provide_context=True,
    )

    # Definindo a ordem das tasks
    task_carregar_arquivo >> task_separar_candles >> task_clusterizar_candles >> task_plot_clusters
