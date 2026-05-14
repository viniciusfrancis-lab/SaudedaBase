from pathlib import Path
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pygei.seges import alunos
import time
import sys

inicio = time.time()

SAVE_FOLDER = Path(__file__).parent.parent / "dados"
NUM_ANO_LETIVO = '2026'

COLUNAS_ALVO = [
    'id_aluno', 'nm_aluno', 'data_nascimento', 'inep_escola',
    'nm_escola', 'nm_regional', 'id_ano_letivo', 'num_ano_letivo', 'dt_matricula',
    'dt_enturmacao', 'situacao_enturmacao', 'situacao_matricula', 'tipo_atendimento',
    'fl_deficiencia', 'dc_deficiencia', 'cpf', 'dc_cor_raca', "nome_ano_escolaridade",
    "ano_escolaridade", "nome_turma", "dc_turno", 'nm_filiacao_1', 'nm_filiacao_2', 'data_referencia'
]

def geracao(n_ano_letivo: str, saveFolder: Path = SAVE_FOLDER, colunas_alvo: list[str] = COLUNAS_ALVO):
    available_dates = [date for date in alunos.available_dates() if date.year == int(NUM_ANO_LETIVO)]

    dtype_dict = {
        'id_aluno': 'Int64',
        'inep_escola': 'Int64',
        'id_ano_letivo': 'Int64'
    }

    acumulado_base = []

    for date in available_dates:
        df = alunos.load(date.year, date.month, date.day)
        df = df.astype(dtype_dict).copy()
        df.cpf = df.cpf.str.strip().str.replace(".", "").str.replace("-", "").str.zfill(11)
        df = df.seges.ativas()
        df.num_ano_letivo = df.num_ano_letivo.astype(str)
        df = df.seges.por_ano(n_ano_letivo)

        numero_ano_letivo = [ano_letivo for ano_letivo in df.num_ano_letivo.unique() if 'MAPES' not in ano_letivo or 'CEET' not in ano_letivo]
        df = df[df.num_ano_letivo.isin(numero_ano_letivo)].copy()

        df['data_referencia'] = date
        df = df[colunas_alvo]
        acumulado_base.append(df)

    dfmatricula = pd.concat(acumulado_base)
    table = pa.Table.from_pandas(dfmatricula)
    saveFile = saveFolder / 'EMPILHADO_MATRICULAS.parquet'
    return pq.write_table(table, saveFile)


if __name__ == "__main__":
    try:
        geracao(NUM_ANO_LETIVO, SAVE_FOLDER, COLUNAS_ALVO)
        fim = time.time()
        print(f"\n✅ Executado com sucesso! Tempo total: {fim - inicio:.2f} segundos")
    except Exception as e:
        fim = time.time()
        print(f"\n❌ Erro após {fim - inicio:.2f} segundos: {e}")
        sys.exit(1)