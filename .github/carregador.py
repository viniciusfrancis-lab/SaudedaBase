#carregador.py
import pandas as pd

COLUNAS_OBRIGATORIAS = ["cpf", "id_aluno", "id_turma", "nm_aluno", "data_nascimento"]


def carregar_base(nome_arquivo) -> pd.DataFrame:
    """
    Lê uma base de base de empilhada alunos.
    """

    df = pd.read_parquet(nome_arquivo)

    df.columns = df.columns.str.strip().str.lower()

    return df

# filtro geral para outros alertas
def aplicar_filtros(df: pd.DataFrame) -> pd.DataFrame:
    # Apenas registros do ano letivo 2026 (exceto MEPES)
    anos_2026 = [v for v in df["num_ano_letivo"].unique() if "2026" in str(v)]
    df = df[df["num_ano_letivo"].isin(anos_2026)]
    df = df[df["num_ano_letivo"] != "2026 - MEPES"]

    # Apenas matrículas e enturmações em curso
    df = df[df["situacao_enturmacao"] == "Em curso"]
    df = df[df["situacao_matricula"] == "Em curso"]

    # Sem data de encerramento
    df = df[df["data_encerramento_matricula"].isna()]

    # Apenas atendimento regular
    df = df[df["tipo_atendimento"] == "Regular"]

    return df.reset_index(drop=True)

#filtro exclusivo para o CPF, tipo que atendimento sao todos
def aplicar_filtros_cpf(df: pd.DataFrame) -> pd.DataFrame:
    # Apenas registros do ano letivo 2026 (exceto MEPES)
    anos_2026 = [v for v in df["num_ano_letivo"].unique() if "2026" in str(v)]
    df = df[df["num_ano_letivo"].isin(anos_2026)]
    df = df[df["num_ano_letivo"] != "2026 - MEPES"]

    # Apenas matrículas e enturmações em curso
    df = df[df["situacao_enturmacao"] == "Em curso"]
    df = df[df["situacao_matricula"] == "Em curso"]

    # Sem data de encerramento
    df = df[df["data_encerramento_matricula"].isna()]

    # Remove linhas onde id_aluno já apareceu antes
    df = df.drop_duplicates(subset=['id_aluno'], keep='first')

    return df.reset_index(drop=True)
