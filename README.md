# Stream Data Pipeline

[![Go Version](https://img.shields.io/badge/Go-1.22%2B-00ADD8?logo=go)](https://golang.org)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

Motor concorrente para ingestão de telemetria, agregação em janelas de tempo deslizantes e cálculo estatístico de métricas de Qualidade de Serviço (QoS) em Go.

Projetado para experimentação e estudos de arquitetura de processamento de fluxo contínuo de dados com baixa latência.

## Arquitetura

```mermaid
flowchart LR
    Ingest[HTTP Ingest] --> InChan[Buffered In-Channel]
    subgraph Workers["Worker Pool Concorrente"]
        W1[Worker 1]
        W2[Worker 2]
    end
    InChan --> Workers
    Workers --> Filter[Predicados & Filtros]
    Filter --> Transform[Transformações & Normalização]
    Transform --> OutChan[Buffered Out-Channel]
    OutChan --> Storage[In-Memory TimeSeries Store]
    Storage --> Analytics[Estatísticas de Janela / P50, P90, P99 / EMA]
    Analytics --> Query[Query API / Prometheus]
```

## Como Executar

```bash
make test
make test-bench
make run
```

## API Endpoints

| Método | Endpoint | Descrição |
|---|---|---|
| `POST` | `/api/v1/events` | Ingestão assíncrona de evento de telemetria |
| `GET` | `/api/v1/metrics` | Listagem de métricas registradas |
| `GET` | `/api/v1/metrics/query` | Consulta histórica com filtro temporal e tags |
| `GET` | `/api/v1/metrics/aggregate` | Agregação estatística de janela (Média, P50, P90, P99) |
| `GET` | `/health` | Healthcheck simples |
| `GET` | `/metrics` | Métricas operacionais em formato Prometheus |

## Autor

Silas Medeiros ([@silasms](https://github.com/silasms))  
silas.medeiros7@gmail.com
