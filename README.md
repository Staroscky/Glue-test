# 📊 Glue Job - Extração de IDs por Matching de CPF

## 📋 Índice

- [Visão Geral](#visão-geral)
- [Responsabilidades](#responsabilidades)
- [Arquitetura](#arquitetura)
- [Conceitos Principais](#conceitos-principais)
- [Fluxo de Execução](#fluxo-de-execução)
- [Configuração](#configuração)
- [Manutenção](#manutenção)

---

## 🎯 Visão Geral

Este job AWS Glue realiza a **extração diária de IDs** da Tabela B com base em matching de CPF com a Tabela A, gerando um arquivo comprimido `.txt.gz` no S3 para consumo por serviços downstream.

### **Contexto de Negócio**

O job é parte crítica do pipeline de dados, alimentando serviços essenciais que dependem de uma lista atualizada diariamente de IDs elegíveis. A integridade e disponibilidade deste arquivo impactam diretamente operações de negócio.

### **Características Principais**

- ✅ **Resultado controlado**: Saída limitada a ~100k IDs (tamanho da Tabela A)
- ✅ **Validação rigorosa**: Múltiplas camadas de validação de integridade
- ✅ **Sobrescrita atômica**: Garante que o arquivo active nunca fique corrompido
- ✅ **Rastreabilidade**: Metadados completos e logs detalhados

---

## 🎭 Responsabilidades

### **O que o Job FAZ:**

1. **Identificar partição mais recente** da Tabela B via Glue Catalog
2. **Ler apenas a partição necessária** usando push-down predicate
3. **Realizar JOIN otimizado** entre Tabela A (100k) e Tabela B (300M)
4. **Extrair IDs únicos** que fazem match entre as tabelas
5. **Gerar arquivo comprimido** `.txt.gz` com um ID por linha
6. **Validar integridade** através de checksums MD5 e contagens
7. **Mover atomicamente** para pasta active sobrescrevendo versão anterior
8. **Registrar metadados** (quantidade, checksum, partição origem, timestamp)

### **O que o Job NÃO FAZ:**

- ❌ Não processa todas as partições da Tabela B (apenas a última)
- ❌ Não faz transformações nos dados (apenas extração)
- ❌ Não mantém histórico de versões (sobrescreve latest.txt.gz)
- ❌ Não notifica falhas automaticamente (requer CloudWatch Alarms)
- ❌ Não valida qualidade dos dados de origem

---

## 🏗️ Arquitetura

### **Diagrama de Fluxo**

```
┌─────────────────────────────────────────────────────────────────┐
│                         AWS GLUE JOB                            │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  1. IDENTIFICAR PARTIÇÃO                                        │
│     ┌──────────────────┐                                        │
│     │  Glue Catalog    │ ← get_partitions()                     │
│     │  (Metadados)     │ → última partição: 20251022            │
│     └──────────────────┘                                        │
│                                                                 │
│  2. LEITURA OTIMIZADA                                           │
│     ┌──────────────────┐     ┌──────────────────┐              │
│     │   Tabela A       │     │   Tabela B       │              │
│     │   100k registros │     │   300M registros │              │
│     │   S3/Parquet     │     │   S3/Parquet     │              │
│     └──────────────────┘     └──────────────────┘              │
│              │                        │                         │
│              │ push-down predicate    │                         │
│              │ (ano_mes_dia=última)   │                         │
│              ↓                        ↓                         │
│     ┌────────────────────────────────────┐                     │
│     │      BROADCAST HASH JOIN           │                     │
│     │  b.documento = CONCAT('000',       │                     │
│     │                a.documento)        │                     │
│     └────────────────────────────────────┘                     │
│                      ↓                                          │
│              ~100k IDs únicos                                   │
│                                                                 │
│  3. GERAÇÃO DE ARQUIVO                                          │
│     ┌──────────────────┐                                        │
│     │  stage/          │                                        │
│     │  stage_*.txt.gz  │ ← Arquivo temporário                   │
│     └──────────────────┘                                        │
│              │                                                  │
│              │ Validações:                                      │
│              │ • IDs vazios                                     │
│              │ • MD5 checksum                                   │
│              │ • Contagem                                       │
│              │ • Tamanho > 0                                    │
│              ↓                                                  │
│     ┌──────────────────┐                                        │
│     │  active/         │                                        │
│     │  latest.txt.gz   │ ← Sobrescrita atômica                  │
│     └──────────────────┘                                        │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### **Estrutura S3**

```
s3://seu-bucket/
├── stage/
│   ├── stage_20251022_120000.txt.gz  ← Arquivos temporários (mantidos para auditoria)
│   ├── stage_20251021_120000.txt.gz
│   └── ...
└── active/
    └── latest.txt.gz  ← Arquivo consumido por serviços (sempre atualizado)
```

---

## 💡 Conceitos Principais

### **1. Push-Down Predicate**

**O que é:** Técnica de otimização que aplica filtros diretamente na leitura dos dados, antes de carregar na memória.

**Por que usar:**
```python
# ❌ SEM push-down: Lê 300M registros → filtra
df_b = glueContext.create_dynamic_frame.from_catalog(...)
df_b_filtrado = df_b.filter(col("ano_mes_dia") == "20251022")  # Tarde demais!

# ✅ COM push-down: Lê apenas a partição necessária
df_b = glueContext.create_dynamic_frame.from_catalog(
    push_down_predicate="ano_mes_dia='20251022'"  # Filtro aplicado na leitura!
)
```

**Benefício:** Economia de 99% no I/O se houver 100 partições diárias.

---

### **2. Broadcast Join**

**O que é:** Estratégia de JOIN onde a tabela menor é copiada inteira para cada worker, evitando shuffle de rede.

**Como funciona:**

```
JOIN NORMAL (Shuffle):                 BROADCAST JOIN:
┌─────────┐  ┌─────────┐              ┌──────────────────┐
│ Worker1 │  │ Worker2 │              │  Tabela A (100k) │
│ A + B   │  │ A + B   │              │   [BROADCAST]    │
└─────────┘  └─────────┘              └──────────────────┘
     ↕            ↕                      ↓    ↓    ↓    ↓
  SHUFFLE      SHUFFLE               ┌───┐┌───┐┌───┐┌───┐
  (LENTO)      (LENTO)               │ W1││ W2││ W3││ W4│
                                     │ B ││ B ││ B ││ B │
                                     └───┘└───┘└───┘└───┘
                                     (RÁPIDO - sem shuffle)
```

**Quando usar:**
- Tabela pequena < 100 MB
- Relação de tamanho > 100:1

**No nosso caso:**
- Tabela A: ~1 MB (100k registros)
- Tabela B: ~100 GB (300M registros)
- **Perfeito para broadcast!**

```python
resultado = df_b.join(
    broadcast(df_a),  # ← Force broadcast da tabela menor
    df_b["documento"] == df_a["documento_formatado"],
    "inner"
)
```

---

### **3. MD5 Checksum**

**O que é:** Função hash criptográfica que gera uma "impressão digital" de 32 caracteres para qualquer conteúdo.

**Propriedades:**
- Qualquer mudança (até 1 bit) → MD5 completamente diferente
- Mesmo conteúdo → Sempre o mesmo MD5
- Impossível recriar o conteúdo original a partir do MD5

**Uso no Job:**
```python
# Calcular antes de enviar
conteudo = "ID001\nID002\nID003"
md5_original = hashlib.md5(conteudo.encode('utf-8')).hexdigest()
# → 'f3d5c8a...' (32 caracteres hex)

# Baixar do S3 e recalcular
conteudo_s3 = baixar_do_s3()
md5_s3 = hashlib.md5(conteudo_s3.encode('utf-8')).hexdigest()

# Validar integridade
if md5_original == md5_s3:
    print("✅ Arquivo íntegro!")
else:
    raise Exception("❌ Arquivo corrompido!")
```

**Por que é importante:** Garante que o arquivo não foi corrompido durante upload/download ou armazenamento no S3.

---

### **4. Sobrescrita Atômica no S3**

**O que é:** Técnica que garante que o arquivo destino nunca fique em estado inconsistente durante atualização.

**Problema sem atomicidade:**
```
1. Serviço lê latest.txt.gz  ✅ Versão antiga (válida)
2. Job começa a escrever...  ⚠️  Arquivo corrompido (50% escrito)
3. Serviço lê latest.txt.gz  ❌ ERRO! Arquivo corrompido
```

**Solução com staging:**
```python
# 1. Escrever em arquivo temporário (stage)
upload("stage/stage_20251022.txt.gz")

# 2. Validar integridade
validar_md5()
validar_contagem()

# 3. Copiar atomicamente (operação atômica do S3)
s3_client.copy_object(
    CopySource="stage/stage_20251022.txt.gz",
    Key="active/latest.txt.gz"  # ← S3 garante atomicidade
)
```

**Resultado:** Serviços sempre veem arquivo válido (versão antiga OU nova completa, nunca parcial).

---

### **5. Distinct (Deduplicação)**

**O que é:** Remove registros duplicados do resultado.

**Por que usar:**
```python
# Cenário: Mesmo CPF pode aparecer múltiplas vezes em B
Tabela A: CPF = 12345678901
Tabela B: 
  - ID001, CPF = 00012345678901 (transação 1)
  - ID002, CPF = 00012345678901 (transação 2)
  - ID003, CPF = 00012345678901 (transação 3)

# Sem distinct: Retorna IDs repetidos
resultado = ["ID001", "ID001", "ID001"]

# Com distinct: Retorna IDs únicos
resultado.distinct() → ["ID001"]
```

**Implementação:**
```python
resultado = df_b.join(...).select(col("id")).distinct()
```

---

## 🔄 Fluxo de Execução

### **Passo a Passo Detalhado**

#### **FASE 1: Inicialização (30s)**
```
1. Spark inicializa cluster
2. Glue Context é criado
3. Conexões S3 e Glue Catalog estabelecidas
```

#### **FASE 2: Identificação de Partição (5s)**
```
1. Consulta Glue Catalog via API (get_partitions)
2. Lista todas as partições da Tabela B
3. Identifica a maior (última): ano_mes_dia = MAX(partições)
4. Log: "Última partição: 20251022"
```

#### **FASE 3: Leitura de Dados (2-3 min)**
```
┌─ Tabela A (rápido: 100k registros)
│  1. Leitura do S3
│  2. Parse Parquet → DataFrame Spark
│  3. Adiciona coluna: documento_formatado = CONCAT('000', documento)
│  
└─ Tabela B (otimizado: push-down predicate)
   1. Leitura APENAS da partição 20251022
   2. Spark divide leitura entre workers
   3. Parse Parquet → DataFrame Spark
   
Total lido: ~1 GB (partição única) ao invés de ~100 GB (todas partições)
```

#### **FASE 4: Processamento - JOIN (3-5 min)**
```
1. Broadcast da Tabela A para todos os workers
   ┌────────────┐
   │ Tabela A   │ → Copiada para todos os workers
   │ (1 MB)     │
   └────────────┘
   
2. Join distribuído
   Worker 1: Processa 75M registros de B + cópia de A
   Worker 2: Processa 75M registros de B + cópia de A
   Worker 3: Processa 75M registros de B + cópia de A
   Worker 4: Processa 75M registros de B + cópia de A
   
3. Filtra apenas matches
   300M registros → ~100k matches
   
4. Remove duplicatas (.distinct())
   ~100k → ~100k únicos
```

#### **FASE 5: Coleta e Validação (30s)**
```
1. Collect IDs para driver
   100k strings × 10 chars = ~1 MB (cabe na memória)
   
2. Validação 1: IDs vazios
   ✓ Nenhum ID vazio encontrado
   
3. Criação do conteúdo
   conteudo = "ID001\nID002\n...\nID100000"
   
4. Cálculo MD5
   md5_original = hashlib.md5(conteudo)
```

#### **FASE 6: Compressão e Upload (1 min)**
```
1. Compressão GZIP
   1 MB (texto) → ~400 KB (comprimido ~60%)
   
2. Upload para stage/
   s3://bucket/stage/stage_20251022_120000.txt.gz
   
3. Validação de upload
   - Baixa arquivo
   - Recalcula MD5
   - Compara: md5_original == md5_s3 ✓
   - Conta linhas: 100k == 100k ✓
   - Verifica tamanho: 400 KB > 0 ✓
```

#### **FASE 7: Movimentação Atômica (10s)**
```
1. S3 Copy (operação atômica)
   stage/stage_20251022_120000.txt.gz 
   → active/latest.txt.gz
   
2. Metadados adicionados
   total_ids: "100000"
   md5_checksum: "f3d5c8a..."
   created_at: "20251022_120000"
   source_partition: "20251022"
   
3. Validação final
   Tamanho active == Tamanho stage ✓
```

#### **FASE 8: Finalização (5s)**
```
1. Log de resumo
2. Métricas CloudWatch
3. Job commit
4. Spark shutdown
```

### **Tempo Total Estimado: 8-12 minutos**

---

## ⚙️ Configuração

### **IAM Role - Permissões Necessárias**

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject",
        "s3:ListBucket"
      ],
      "Resource": [
        "arn:aws:s3:::seu-bucket/*",
        "arn:aws:s3:::seu-bucket"
      ]
    },
    {
      "Effect": "Allow",
      "Action": [
        "glue:GetDatabase",
        "glue:GetTable",
        "glue:GetPartitions"
      ],
      "Resource": [
        "arn:aws:glue:*:*:catalog",
        "arn:aws:glue:*:*:database/seu_database",
        "arn:aws:glue:*:*:table/seu_database/*"
      ]
    },
    {
      "Effect": "Allow",
      "Action": [
        "logs:CreateLogGroup",
        "logs:CreateLogStream",
        "logs:PutLogEvents"
      ],
      "Resource": "arn:aws:logs:*:*:/aws-glue/*"
    }
  ]
}
```

## 📚 Referências

- [AWS Glue Documentation](https://docs.aws.amazon.com/glue/)
- [PySpark SQL Guide](https://spark.apache.org/docs/latest/sql-programming-guide.html)
- [S3 Best Practices](https://docs.aws.amazon.com/AmazonS3/latest/userguide/optimizing-performance.html)
- [Glue Job Parameters](https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-etl-glue-arguments.html)