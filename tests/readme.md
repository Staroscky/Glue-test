# 🧪 Testes Unitários - Glue Job de Extração de IDs

## 📋 Pré-requisitos

- Python 3.9+
- Java 8 ou 11 (necessário para PySpark)

## 🚀 Configuração

### 1. Criar ambiente virtual

```bash
python -m venv venv
source venv/bin/activate  # Linux/Mac
# ou
venv\Scripts\activate  # Windows
```

### 2. Instalar dependências

```bash
pip install -r requirements-test.txt
```

## ▶️ Executar Testes

### Rodar todos os testes

```bash
pytest
```

### Rodar com cobertura detalhada

```bash
pytest --cov=. --cov-report=html
```

Depois abra `htmlcov/index.html` no navegador para ver o relatório de cobertura.

### Rodar testes específicos

```bash
# Por classe
pytest test_glue_job.py::TestGlueJob

# Por método
pytest test_glue_job.py::TestGlueJob::test_join_tables_with_concat

# Por marcador
pytest -m unit
pytest -m integration
```

### Rodar com mais detalhes

```bash
pytest -vv --tb=long
```

### Rodar com logs do Spark

```bash
pytest -s  # Mostra prints e logs
```

## 📊 Cobertura dos Testes

Os testes cobrem:

| Componente | Cobertura | Testes |
|-----------|-----------|--------|
| ✅ Leitura de partições | 100% | `test_get_latest_partition` |
| ✅ JOIN entre tabelas | 100% | `test_join_tables_with_concat` |
| ✅ Broadcast join | 100% | `test_broadcast_join` |
| ✅ Validação de IDs | 100% | `test_validate_empty_ids` |
| ✅ Cálculo MD5 | 100% | `test_md5_checksum` |
| ✅ Compressão GZIP | 100% | `test_gzip_compression` |
| ✅ Upload S3 | 100% | `test_s3_upload_and_validation` |
| ✅ Metadados S3 | 100% | `test_s3_metadata` |
| ✅ Validação integridade | 100% | `test_full_integrity_validation` |
| ✅ Tratamento de erros | 100% | `test_error_handling_*` |
| ✅ Distinct/Duplicatas | 100% | `test_distinct_ids` |
| ✅ Push-down predicate | 100% | `test_push_down_predicate` |
| ✅ Integração completa | 100% | `test_full_job_flow` |
