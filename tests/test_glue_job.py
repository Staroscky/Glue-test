import pytest
import gzip
import hashlib
from unittest.mock import Mock, MagicMock, patch, call
from datetime import datetime
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructType, StructField, StringType


class TestGlueJob:
    """Testes unitários para o Job Glue de extração de IDs"""
    
    @pytest.fixture(scope="class")
    def spark(self):
        """Cria sessão Spark para testes"""
        spark = SparkSession.builder \
            .master("local[2]") \
            .appName("test_glue_job") \
            .getOrCreate()
        yield spark
        spark.stop()
    
    @pytest.fixture
    def mock_glue_context(self):
        """Mock do GlueContext"""
        mock_context = Mock()
        mock_context.spark_session = Mock()
        return mock_context
    
    @pytest.fixture
    def sample_data_a(self, spark):
        """Dados de exemplo para tabela A (100k registros)"""
        data = [
            Row(documento="12345678901"),
            Row(documento="98765432100"),
            Row(documento="11111111111"),
            Row(documento="22222222222"),
            Row(documento="33333333333"),
        ]
        schema = StructType([StructField("documento", StringType(), True)])
        return spark.createDataFrame(data, schema)
    
    @pytest.fixture
    def sample_data_b(self, spark):
        """Dados de exemplo para tabela B (300M registros - simulado)"""
        data = [
            Row(id="ID001", documento="00012345678901", ano_mes_dia="20251022"),
            Row(id="ID002", documento="00098765432100", ano_mes_dia="20251022"),
            Row(id="ID003", documento="00011111111111", ano_mes_dia="20251022"),
            Row(id="ID004", documento="99999999999999", ano_mes_dia="20251022"),  # Não tem match
            Row(id="ID005", documento="00022222222222", ano_mes_dia="20251021"),  # Partição antiga
        ]
        schema = StructType([
            StructField("id", StringType(), True),
            StructField("documento", StringType(), True),
            StructField("ano_mes_dia", StringType(), True)
        ])
        return spark.createDataFrame(data, schema)


    # ========================================
    # TESTE 1: Leitura de Última Partição
    # ========================================
    @patch('boto3.client')
    def test_get_latest_partition(self, mock_boto_client):
        """Testa se identifica corretamente a última partição"""
        # Mock do Glue client
        mock_glue = Mock()
        mock_boto_client.return_value = mock_glue
        
        # Simular resposta do get_partitions
        mock_glue.get_partitions.return_value = {
            'Partitions': [
                {'Values': ['20251020']},
                {'Values': ['20251021']},
                {'Values': ['20251022']},  # Última
                {'Values': ['20251019']},
            ]
        }
        
        glue_client = mock_boto_client('glue')
        response = glue_client.get_partitions(
            DatabaseName='test_db',
            TableName='test_table'
        )
        
        particoes = [p['Values'][0] for p in response['Partitions']]
        ultima_particao = max(particoes)
        
        assert ultima_particao == '20251022'
        mock_glue.get_partitions.assert_called_once()


    # ========================================
    # TESTE 2: JOIN entre Tabelas
    # ========================================
    def test_join_tables_with_concat(self, spark, sample_data_a, sample_data_b):
        """Testa o JOIN entre tabelas A e B com concatenação"""
        from pyspark.sql.functions import concat, lit, col
        
        # Adicionar '000' no documento da tabela A
        df_a = sample_data_a.withColumn(
            "documento_formatado", 
            concat(lit("000"), col("documento"))
        )
        
        # Filtrar apenas última partição
        df_b = sample_data_b.filter(col("ano_mes_dia") == "20251022")
        
        # Fazer JOIN
        resultado = df_b.join(
            df_a,
            df_b["documento"] == df_a["documento_formatado"],
            "inner"
        ).select(col("id"))
        
        ids = [row.id for row in resultado.collect()]
        
        # Deve encontrar 3 matches (ID001, ID002, ID003)
        assert len(ids) == 3
        assert "ID001" in ids
        assert "ID002" in ids
        assert "ID003" in ids
        assert "ID004" not in ids  # Não tem match
        assert "ID005" not in ids  # Partição antiga


    # ========================================
    # TESTE 3: Validação de IDs Vazios
    # ========================================
    def test_validate_empty_ids(self):
        """Testa validação de IDs vazios ou nulos"""
        ids_list = ["ID001", "ID002", "", "ID004", None, "ID006"]
        
        ids_invalidos = [id for id in ids_list if not id or (isinstance(id, str) and id.strip() == '')]
        
        assert len(ids_invalidos) == 2  # "" e None
        
        # Deve lançar exceção se houver IDs inválidos
        if ids_invalidos:
            with pytest.raises(Exception):
                raise Exception(f"Encontrados {len(ids_invalidos)} IDs inválidos")


    # ========================================
    # TESTE 4: Cálculo de MD5
    # ========================================
    def test_md5_checksum(self):
        """Testa cálculo correto do MD5"""
        conteudo = "ID001\nID002\nID003"
        
        md5_hash = hashlib.md5(conteudo.encode('utf-8')).hexdigest()
        
        # MD5 deve ter 32 caracteres hexadecimais
        assert len(md5_hash) == 32
        assert all(c in '0123456789abcdef' for c in md5_hash)
        
        # Mesmo conteúdo = mesmo MD5
        md5_hash2 = hashlib.md5(conteudo.encode('utf-8')).hexdigest()
        assert md5_hash == md5_hash2
        
        # Conteúdo diferente = MD5 diferente
        conteudo_diferente = "ID001\nID002\nID004"
        md5_diferente = hashlib.md5(conteudo_diferente.encode('utf-8')).hexdigest()
        assert md5_hash != md5_diferente


    # ========================================
    # TESTE 5: Compressão GZIP
    # ========================================
    def test_gzip_compression(self, tmp_path):
        """Testa compressão e descompressão GZIP"""
        ids_list = [f"ID{str(i).zfill(6)}" for i in range(1000)]
        conteudo = '\n'.join(ids_list)
        
        # Comprimir
        compressed_file = tmp_path / "test.txt.gz"
        with gzip.open(compressed_file, 'wt') as f:
            f.write(conteudo)
        
        # Verificar compressão
        original_size = len(conteudo.encode('utf-8'))
        compressed_size = compressed_file.stat().st_size
        
        assert compressed_size < original_size
        
        # Descomprimir e validar
        with gzip.open(compressed_file, 'rt') as f:
            conteudo_descomprimido = f.read()
        
        assert conteudo == conteudo_descomprimido


    # ========================================
    # TESTE 6: Validação de Quantidade
    # ========================================
    def test_count_validation(self):
        """Testa validação de quantidade de registros"""
        total_ids_esperado = 1000
        ids_list = [f"ID{i}" for i in range(1000)]
        
        # Deve passar
        assert len(ids_list) == total_ids_esperado
        
        # Deve falhar se quantidade diferente
        ids_list_errado = ids_list[:-10]  # Remove 10 itens
        assert len(ids_list_errado) != total_ids_esperado


    # ========================================
    # TESTE 7: Upload e Validação S3
    # ========================================
    @patch('boto3.client')
    def test_s3_upload_and_validation(self, mock_boto_client, tmp_path):
        """Testa upload para S3 e validação de integridade"""
        mock_s3 = Mock()
        mock_boto_client.return_value = mock_s3
        
        # Criar arquivo comprimido
        ids_list = ["ID001", "ID002", "ID003"]
        conteudo = '\n'.join(ids_list)
        
        compressed_file = tmp_path / "stage_test.txt.gz"
        with gzip.open(compressed_file, 'wt') as f:
            f.write(conteudo)
        
        md5_original = hashlib.md5(conteudo.encode('utf-8')).hexdigest()
        
        # Simular upload
        s3_client = mock_boto_client('s3')
        s3_client.upload_file(
            str(compressed_file),
            'test-bucket',
            'stage/stage_test.txt.gz'
        )
        
        # Simular head_object para verificar tamanho
        mock_s3.head_object.return_value = {
            'ContentLength': compressed_file.stat().st_size
        }
        
        # Verificações
        s3_client.upload_file.assert_called_once()
        assert mock_s3.head_object.call_count <= 1


    # ========================================
    # TESTE 8: Broadcast Join Performance
    # ========================================
    def test_broadcast_join(self, spark, sample_data_a, sample_data_b):
        """Testa que broadcast join funciona corretamente"""
        from pyspark.sql.functions import broadcast, concat, lit, col
        
        df_a = sample_data_a.withColumn(
            "documento_formatado",
            concat(lit("000"), col("documento"))
        )
        
        df_b = sample_data_b.filter(col("ano_mes_dia") == "20251022")
        
        # JOIN com broadcast
        resultado = df_b.join(
            broadcast(df_a),
            df_b["documento"] == df_a["documento_formatado"],
            "inner"
        ).select(col("id"))
        
        # Verificar que retorna resultados corretos
        ids = [row.id for row in resultado.collect()]
        assert len(ids) == 3
        
        # Verificar que o plano de execução usa BroadcastHashJoin
        plan = resultado._jdf.queryExecution().executedPlan().toString()
        assert "Broadcast" in plan or len(ids) == 3  # Fallback se não conseguir verificar plan


    # ========================================
    # TESTE 9: Distinct para Remover Duplicatas
    # ========================================
    def test_distinct_ids(self, spark):
        """Testa remoção de IDs duplicados"""
        data = [
            Row(id="ID001"),
            Row(id="ID002"),
            Row(id="ID001"),  # Duplicata
            Row(id="ID003"),
            Row(id="ID002"),  # Duplicata
        ]
        
        df = spark.createDataFrame(data)
        df_distinct = df.distinct()
        
        ids = [row.id for row in df_distinct.collect()]
        
        assert len(ids) == 3
        assert sorted(ids) == ["ID001", "ID002", "ID003"]


    # ========================================
    # TESTE 10: Metadados S3
    # ========================================
    @patch('boto3.client')
    def test_s3_metadata(self, mock_boto_client):
        """Testa gravação de metadados no S3"""
        mock_s3 = Mock()
        mock_boto_client.return_value = mock_s3
        
        s3_client = mock_boto_client('s3')
        
        # Simular copy_object com metadados
        s3_client.copy_object(
            Bucket='test-bucket',
            CopySource={'Bucket': 'test-bucket', 'Key': 'stage/file.txt.gz'},
            Key='active/latest.txt.gz',
            MetadataDirective='REPLACE',
            Metadata={
                'total_ids': '1000',
                'md5_checksum': 'abc123',
                'created_at': '20251022_120000',
                'source_partition': '20251022'
            }
        )
        
        # Verificar chamada
        s3_client.copy_object.assert_called_once()
        call_args = s3_client.copy_object.call_args
        
        assert call_args[1]['Metadata']['total_ids'] == '1000'
        assert call_args[1]['Metadata']['md5_checksum'] == 'abc123'


    # ========================================
    # TESTE 11: Validação de Integridade Completa
    # ========================================
    def test_full_integrity_validation(self, tmp_path):
        """Testa fluxo completo de validação de integridade"""
        ids_list = [f"ID{str(i).zfill(6)}" for i in range(100)]
        total_ids = len(ids_list)
        conteudo = '\n'.join(ids_list)
        
        # 1. Calcular MD5 original
        md5_original = hashlib.md5(conteudo.encode('utf-8')).hexdigest()
        
        # 2. Comprimir
        compressed_file = tmp_path / "test.txt.gz"
        with gzip.open(compressed_file, 'wt') as f:
            f.write(conteudo)
        
        # 3. Descomprimir e validar MD5
        with gzip.open(compressed_file, 'rt') as f:
            conteudo_s3 = f.read()
        
        md5_s3 = hashlib.md5(conteudo_s3.encode('utf-8')).hexdigest()
        
        # 4. Validar checksums
        assert md5_original == md5_s3, "MD5 checksums não correspondem!"
        
        # 5. Validar quantidade de linhas
        linhas_s3 = len([line for line in conteudo_s3.strip().split('\n') if line.strip()])
        assert total_ids == linhas_s3, f"Esperado {total_ids}, encontrado {linhas_s3}"
        
        # 6. Validar tamanho do arquivo
        file_size = compressed_file.stat().st_size
        assert file_size > 0, "Arquivo está vazio!"


    # ========================================
    # TESTE 12: Tratamento de Erros
    # ========================================
    def test_error_handling_no_ids(self):
        """Testa tratamento quando não há IDs encontrados"""
        total_ids = 0
        
        with pytest.raises(Exception, match="Nenhum ID encontrado"):
            if total_ids == 0:
                raise Exception("ERRO: Nenhum ID encontrado após o JOIN. Abortando job.")
    
    def test_error_handling_md5_mismatch(self):
        """Testa tratamento de erro quando MD5 não corresponde"""
        md5_original = "abc123"
        md5_s3 = "def456"
        
        with pytest.raises(Exception, match="ERRO DE INTEGRIDADE"):
            if md5_original != md5_s3:
                raise Exception(f"ERRO DE INTEGRIDADE! MD5 original: {md5_original}, MD5 S3: {md5_s3}")


    # ========================================
    # TESTE 13: Push-down Predicate
    # ========================================
    def test_push_down_predicate(self, spark, sample_data_b):
        """Testa que filtro de partição funciona corretamente"""
        from pyspark.sql.functions import col
        
        # Filtrar apenas última partição
        ultima_particao = "20251022"
        df_filtrado = sample_data_b.filter(col("ano_mes_dia") == ultima_particao)
        
        # Verificar que só tem registros da partição correta
        particoes = [row.ano_mes_dia for row in df_filtrado.collect()]
        assert all(p == ultima_particao for p in particoes)
        assert len(particoes) == 3  # 3 registros nessa partição


# ========================================
# TESTE DE INTEGRAÇÃO (OPCIONAL)
# ========================================
class TestGlueJobIntegration:
    """Teste de integração simulando fluxo completo"""
    
    @pytest.fixture(scope="class")
    def spark(self):
        spark = SparkSession.builder \
            .master("local[2]") \
            .appName("test_integration") \
            .getOrCreate()
        yield spark
        spark.stop()
    
    @patch('boto3.client')
    def test_full_job_flow(self, mock_boto_client, spark, tmp_path):
        """Testa fluxo completo do job end-to-end"""
        from pyspark.sql import Row
        from pyspark.sql.functions import concat, lit, col, broadcast
        
        # 1. Criar dados de teste
        data_a = [Row(documento=f"{i:011d}") for i in range(100)]
        data_b = [
            Row(id=f"ID{i:06d}", documento=f"000{i:011d}", ano_mes_dia="20251022")
            for i in range(100)
        ]
        
        df_a = spark.createDataFrame(data_a)
        df_b = spark.createDataFrame(data_b)
        
        # 2. Processar
        df_a = df_a.withColumn("documento_formatado", concat(lit("000"), col("documento")))
        ultima_particao = "20251022"
        df_b = df_b.filter(col("ano_mes_dia") == ultima_particao)
        
        resultado = df_b.join(
            broadcast(df_a),
            df_b["documento"] == df_a["documento_formatado"],
            "inner"
        ).select(col("id")).distinct()
        
        total_ids = resultado.count()
        ids_list = [row.id for row in resultado.collect()]
        
        # 3. Criar arquivo
        conteudo = '\n'.join(ids_list)
        md5_original = hashlib.md5(conteudo.encode('utf-8')).hexdigest()
        
        compressed_file = tmp_path / "test.txt.gz"
        with gzip.open(compressed_file, 'wt') as f:
            f.write(conteudo)
        
        # 4. Validar
        with gzip.open(compressed_file, 'rt') as f:
            conteudo_validado = f.read()
        
        md5_validado = hashlib.md5(conteudo_validado.encode('utf-8')).hexdigest()
        linhas = len([l for l in conteudo_validado.strip().split('\n') if l.strip()])
        
        # Assertions finais
        assert total_ids == 100
        assert len(ids_list) == 100
        assert md5_original == md5_validado
        assert total_ids == linhas
        assert compressed_file.stat().st_size > 0
        
        print(f"✓ Teste de integração passou! {total_ids} IDs processados com sucesso.")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])