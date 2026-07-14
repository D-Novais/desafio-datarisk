# Desafio de Engenharia de Dados - Datarisk

Este repositório contém a solução estruturada para o pipeline de dados de corridas de táxi de Nova York, construído com foco em alta disponibilidade, escalabilidade e boas práticas de modelagem.

## 1. Arquitetura e Decisões Técnicas

A solução foi projetada utilizando a **Arquitetura Medallion** (Raw, Trusted, Refined), garantindo a governança, a qualidade e a rastreabilidade dos dados em cada estágio do fluxo.

*   **Infraestrutura Docker:** Orquestração local via `docker-compose` isolando o Apache Airflow (orquestrador) e o PostgreSQL (Data Warehouse). Um `Dockerfile` customizado foi implementado para injetar o ambiente Java (OpenJRE) na imagem oficial do Airflow.
*   **Extração e Carga Massiva (PySpark):** Para lidar com o processamento de mais de 37 milhões de registros de forma otimizada e contornar limites físicos de I/O e memória RAM, a ingestão (camada Raw) foi arquitetada utilizando processamento distribuído com **Apache Spark** via driver JDBC, substituindo abordagens tradicionais in-memory.
*   **Qualidade e Regras de Negócio (SQL):** As transformações para as camadas Trusted e Refined foram executadas de forma idempotente via `SQLExecuteQueryOperator`, aplicando tipagem forte de dados e filtragem de anomalias (como viagens com distância zerada ou sem passageiros registrados).

---

## 2. Como Executar o Projeto Localmente

1. Clone este repositório e acesse a pasta `challenge`.
2. Garanta que os arquivos compactados `.parquet.gz` do dataset original foram copiados para o diretório `challenge/data/`.
3. Construa a imagem customizada com suporte ao PySpark e inicie a infraestrutura:
   ```bash
   docker compose up -d --build



**4. Acesse o Airflow:**
Navegue até `http://localhost:8080` no seu navegador (Credenciais: `admin` / `admin`).

**5. Configure a Conexão com o Data Warehouse:**
No menu superior do Airflow, vá em **Admin > Connections > +** e preencha os dados abaixo:

| Campo | Valor |
| :--- | :--- |
| **Connection Id** | `postgres_dw_conn` |
| **Connection Type** | `Postgres` |
| **Host** | `postgres-dw` |
| **Schema** | `taxidb` |
| **Login / Password** | `datarisk` |

**6. Ative o Pipeline:**
No painel principal, ative a chave da DAG `taxi_trips_etl` e acompanhe a execução automática das tarefas.

---

## 3. Respostas às Questões de Negócio

As métricas abaixo foram extraídas da camada Refined após o processamento completo do ano de 2022. As queries de transformação e validação encontram-se na pasta `sql/`.

### 1. Qual o total de registros na tabela final? 
* **Resposta:** A tabela final consolidada contém **37.038.525** registros válidos.

```sql
SELECT COUNT(*) AS total_registros 
FROM refined.taxi_trips;

```

### 2. Qual o total de viagens iniciadas e finalizadas no dia 17 de junho?

* **Resposta:** Foram realizadas **114.336** viagens com início e fim exatos nesta data.

```sql
SELECT COUNT(*) AS total_viagens_17_junho
FROM refined.taxi_trips 
WHERE DATE(pickup_datetime) = '2022-06-17' 
  AND DATE(dropoff_datetime) = '2022-06-17';

```

### 3. Qual foi o dia da viagem mais longa percorrida?

* **Resposta:** A maior distância registrada ocorreu no dia **24 de Junho de 2022**, com um total de **184.340,80** milhas.

```sql
SELECT DATE(pickup_datetime) AS data_viagem, trip_distance
FROM refined.taxi_trips
ORDER BY trip_distance DESC
LIMIT 1;

```

### 4. Distribuição da distância percorrida nas viagens totais

Estatísticas descritivas das distâncias percorridas (em milhas):

* **Média:** 3.57

* **Desvio Padrão:** 57.37

* **Mínimo:** 0.01

* **Máximo:** 184340.80

* **Quartis:**
* Q1 (25%): 1.12

* Mediana / Q2 (50%): 1.9

* Q3 (75%): 3.52

```sql
SELECT 
    ROUND(AVG(trip_distance), 2) AS media,
    ROUND(STDDEV(trip_distance), 2) AS desvio_padrao,
    MIN(trip_distance) AS minimo,
    MAX(trip_distance) AS maximo,
    PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY trip_distance) AS q1,
    PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY trip_distance) AS mediana,
    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY trip_distance) AS q3
FROM refined.taxi_trips;

```

```

```
