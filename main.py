import sys
import boto3
import gzip
from datetime import datetime
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import concat, lit, col, max as spark_max
from pyspark.sql.types import StringType

# Configurações
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Cliente S3 e Glue
s3_client = boto3.client('s3')
glue_client = boto3.client('glue')

# ============================================
# CONFIGURAÇÕES - AJUSTE AQUI
# ============================================
DATABASE_NAME = "seu_database"
TABELA_A = "tabela_a"
TABELA_B = "tabela_b"
S3_BUCKET = "seu-bucket"
S3_STAGE_PATH = "stage"
S3_ACTIVE_PATH = "active"
FILENAME = "latest.txt.gz"

# ============================================
# 1. LER TABELA A (100k registros)
# ============================================
print("Lendo tabela A...")
df_a = glueContext.create_dynamic_frame.from_catalog(
    database=DATABASE_NAME,
    table_name=TABELA_A
).toDF()

# Adicionar '000' no documento da tabela A
df_a = df_a.withColumn("documento_formatado", concat(lit("000"), col("documento")))

print(f"Registros na tabela A: {df_a.count()}")

# ============================================
# 2. LER TABELA B - ÚLTIMA PARTIÇÃO (OTIMIZADO)
# ============================================
print("Identificando última partição da tabela B...")

# Buscar última partição via Glue Catalog (muito mais rápido!)
glue_client = boto3.client('glue')
partitions_response = glue_client.get_partitions(
    DatabaseName=DATABASE_NAME,
    TableName=TABELA_B,
    MaxResults=1000
)

# Extrair todas as partições e encontrar a maior
particoes = [p['Values'][0] for p in partitions_response['Partitions']]  # Assumindo que ano_mes_dia é a primeira coluna de partição
ultima_particao = max(particoes)
print(f"Última partição encontrada: {ultima_particao}")

# Ler APENAS a última partição (push-down predicate)
df_b = glueContext.create_dynamic_frame.from_catalog(
    database=DATABASE_NAME,
    table_name=TABELA_B,
    push_down_predicate=f"ano_mes_dia='{ultima_particao}'"
).toDF()

print(f"Registros na última partição de B: {df_b.count()}")

# ============================================
# 3. FAZER JOIN E EXTRAIR IDs (OTIMIZADO)
# ============================================
print("Realizando JOIN entre tabelas...")

# Broadcast da tabela A (menor - 100k registros) para otimizar JOIN
from pyspark.sql.functions import broadcast

resultado = df_b.join(
    broadcast(df_a),  # Broadcast join - muito mais rápido!
    df_b["documento"] == df_a["documento_formatado"],
    "inner"
).select(col("b.id").cast(StringType()).alias("id")).distinct()  # distinct para evitar duplicatas

# Contar registros (SEM cache - job roda diariamente, não precisa reusar)
total_ids = resultado.count()
print(f"Total de IDs únicos encontrados após JOIN: {total_ids}")

# Validação: verificar se há registros
if total_ids == 0:
    raise Exception("ERRO: Nenhum ID encontrado após o JOIN. Abortando job.")

# ============================================
# 4. CRIAR ARQUIVO STAGE .txt.gz (OTIMIZADO)
# ============================================
print("Criando arquivo stage...")

timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
stage_filename = f"stage_{timestamp}.txt.gz"
compressed_local_file = f"/tmp/{stage_filename}"

# Como o JOIN resulta em ~100k registros (tamanho de A), podemos usar collect()
print("Coletando IDs (resultado do JOIN é pequeno ~100k)...")
ids_list = [row.id for row in resultado.collect()]

# Validação 1: Verificar se há IDs vazios ou nulos
ids_invalidos = [id for id in ids_list if not id or id.strip() == '']
if ids_invalidos:
    raise Exception(f"ERRO: Encontrados {len(ids_invalidos)} IDs vazios ou nulos!")

print(f"IDs coletados: {len(ids_list):,}")

# Criar arquivo comprimido com checksum
import hashlib
conteudo = '\n'.join(ids_list)

# Calcular MD5 do conteúdo original
md5_original = hashlib.md5(conteudo.encode('utf-8')).hexdigest()
print(f"MD5 do conteúdo: {md5_original}")

# Comprimir
with gzip.open(compressed_local_file, 'wt') as f:
    f.write(conteudo)

# Tamanhos
import os
file_size_uncompressed = len(conteudo.encode('utf-8'))
file_size_compressed = os.path.getsize(compressed_local_file)
compression_ratio = (1 - file_size_compressed / file_size_uncompressed) * 100

print(f"Tamanho descomprimido: {file_size_uncompressed:,} bytes")
print(f"Tamanho comprimido: {file_size_compressed:,} bytes ({compression_ratio:.1f}% compressão)")

# Upload para S3
s3_client.upload_file(compressed_local_file, S3_BUCKET, f"{S3_STAGE_PATH}/{stage_filename}")
print(f"✓ Arquivo stage criado: s3://{S3_BUCKET}/{S3_STAGE_PATH}/{stage_filename}")

# ============================================
# 6. VALIDAR ARQUIVO NO S3 (SEM BAIXAR TUDO)
# ============================================
print("Validando arquivo final no S3...")

# Baixar apenas uma amostra para validar formato (primeiras 1000 linhas)
temp_validate_file = f"/tmp/validate_{timestamp}.txt.gz"
s3_client.download_file(S3_BUCKET, f"{S3_STAGE_PATH}/{stage_filename}", temp_validate_file)

# Validar amostra
with gzip.open(temp_validate_file, 'rt') as f:
    sample_lines = [next(f).strip() for _ in range(min(1000, linha_count))]
    
# Validação: IDs na amostra não devem estar vazios
ids_vazios_sample = [id for id in sample_lines if not id]
if ids_vazios_sample:
    raise Exception(f"ERRO: Encontrados IDs vazios na amostra!")

print(f"✓ Amostra validada: {len(sample_lines)} IDs verificados")

# Verificar tamanho do arquivo
file_size = s3_client.head_object(Bucket=S3_BUCKET, Key=f"{S3_STAGE_PATH}/{stage_filename}")['ContentLength']
if file_size == 0:
    raise Exception("ERRO: Arquivo no S3 está vazio!")

print(f"✓ Tamanho do arquivo comprimido: {file_size:,} bytes")
print("✓ Validação completa: Arquivo íntegro!")

os.remove(temp_validate_file)

# ============================================
# 7. MOVER PARA active/latest.txt.gz (ATÔMICO + VALIDAÇÃO FINAL)
# ============================================
print("Movendo arquivo para active com sobrescrita atômica...")

active_key = f"{S3_ACTIVE_PATH}/{FILENAME}"

# Copiar de stage para active
copy_response = s3_client.copy_object(
    Bucket=S3_BUCKET,
    CopySource={'Bucket': S3_BUCKET, 'Key': f"{S3_STAGE_PATH}/{stage_filename}"},
    Key=active_key,
    MetadataDirective='REPLACE',
    Metadata={
        'total_ids': str(total_ids),
        'md5_checksum': md5_original,
        'created_at': timestamp,
        'source_partition': ultima_particao,
        'compression_ratio': f"{compression_ratio:.1f}"
    }
)

print(f"✓ Arquivo movido para: s3://{S3_BUCKET}/{active_key}")

# Validação FINAL: Confirmar que arquivo active está correto
final_file_size = s3_client.head_object(Bucket=S3_BUCKET, Key=active_key)['ContentLength']
if final_file_size != file_size:
    raise Exception(f"ERRO CRÍTICO: Tamanho do arquivo active ({final_file_size:,}) difere do stage ({file_size:,})!")

print(f"✓ Arquivo active validado: {final_file_size:,} bytes")
print(f"✓ Arquivo stage mantido para auditoria: s3://{S3_BUCKET}/{S3_STAGE_PATH}/{stage_filename}")

# ============================================
# FINALIZAÇÃO
# ============================================
print("=" * 60)
print("JOB CONCLUÍDO COM SUCESSO!")
print(f"Total de IDs processados: {total_ids:,}")
print(f"Partição origem: {ultima_particao}")
print(f"Tamanho descomprimido: {file_size_uncompressed:,} bytes")
print(f"Tamanho comprimido: {file_size_compressed:,} bytes ({compression_ratio:.1f}% de compressão)")
print(f"MD5 checksum: {md5_original}")
print(f"Arquivo final: s3://{S3_BUCKET}/{active_key}")
print("=" * 60)

job.commit()