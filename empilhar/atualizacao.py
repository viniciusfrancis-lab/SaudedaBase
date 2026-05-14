import os
import pandas as pd
from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq
import logging
from datetime import datetime
from pygei.seges import alunos

# 1. Configuração do Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(), # Mostra no terminal
        # logging.FileHandler('processamento_matriculas.log') # Opcional: salva em arquivo
    ]
)
logger = logging.getLogger(__name__)

SAVE_FOLDER = Path(__file__).parent.parent / "dados"
NUM_ANO_LETIVO = '2026'

COLUNAS_ALVO = [
    'id_aluno', 'nm_aluno', 'data_nascimento', 'inep_escola', 
    'nm_escola', 'nm_regional', 'id_ano_letivo', 'num_ano_letivo', 'dt_matricula', 
    'dt_enturmacao', 'situacao_enturmacao', 'situacao_matricula', 'tipo_atendimento',
    'fl_deficiencia', 'dc_deficiencia', 'cpf', 'dc_cor_raca', "nome_ano_escolaridade",
    "ano_escolaridade", "nome_turma", "dc_turno", 'nm_filiacao_1', 'nm_filiacao_2'
]

def atualizacao(n_ano_letivo: str, saveFolder: Path = SAVE_FOLDER, colunas_alvo: list[str] = COLUNAS_ALVO):
    logger.info(f"Iniciando atualização para o ano letivo: {n_ano_letivo}")
    
    # Busca datas disponíveis no SEGES
    available_dates = [date for date in alunos.available_dates() if date.year == int(n_ano_letivo)]
    logger.info(f"Total de datas encontradas no SEGES: {len(available_dates)}")

    # Carrega arquivo existente
    caminho_arquivo = saveFolder / 'EMPILHADO_MATRICULAS.parquet'
    if caminho_arquivo.exists():
        logger.info("Carregando base empilhada existente...")
        dfEmpilhado = pd.read_parquet(caminho_arquivo)
        DatasMatricula = dfEmpilhado.data_referencia.unique()
    else:
        logger.warning("Arquivo base não encontrado. Criando um novo.")
        dfEmpilhado = pd.DataFrame()
        DatasMatricula = set()

    # Identifica o que falta
    DatasFaltantes = sorted(list(set(available_dates) - set(DatasMatricula)))
    
    if not DatasFaltantes:
        logger.info("A base já está atualizada. Nenhuma data nova encontrada.")
        return

    logger.info(f"Datas faltantes identificadas ({len(DatasFaltantes)}): {DatasFaltantes}")

    acumulado_base = [dfEmpilhado] if not dfEmpilhado.empty else []

    for i, date in enumerate(DatasFaltantes, 1):
        try:
            logger.info(f"[{i}/{len(DatasFaltantes)}] Processando data: {date.strftime('%d/%m/%Y')}...")
            
            df = alunos.load(date.year, date.month, date.day)
            
            # Aplicando filtros
            df = df.seges.ativas()
            df = df[colunas_alvo]
            df.num_ano_letivo = df.num_ano_letivo.astype(str)
            df = df.seges.por_ano(n_ano_letivo)

            # Filtro de strings (MAPES/CEET)
            numero_ano_letivo = [ano for ano in df.num_ano_letivo.unique() if 'MAPES' not in ano and 'CEET' not in ano]
            df = df[df.num_ano_letivo.isin(numero_ano_letivo)]
            
            df['data_referencia'] = date
            acumulado_base.append(df)
            
            logger.info(f"Sucesso: {len(df)} registros adicionados para a data {date.date()}.")
            
        except Exception as e:
            logger.error(f"Erro ao processar a data {date}: {e}")

    # Consolidação final
    logger.info("Concatenando todas as bases...")
    dfmatricula = pd.concat(acumulado_base, ignore_index=True)
    
    logger.info(f"Convertendo para Parquet e salvando em: {caminho_arquivo}")
    table = pa.Table.from_pandas(dfmatricula)
    pq.write_table(table, caminho_arquivo)
    
    logger.info("Atualização concluída com sucesso!")

# Execução
if __name__ == "__main__":
    atualizacao(NUM_ANO_LETIVO)