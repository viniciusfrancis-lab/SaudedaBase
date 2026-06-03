import pandas as pd

def checar_sem_turma(df: pd.DataFrame) -> pd.DataFrame:
    #sem_turma = df["id_turma"].isna() | (df["id_turma"].str.strip() == "")
    sem_turma = df["id_turma"].isna() | (df["id_turma"].astype(str).str.strip().isin(["", "nan", "None"]))
    erros = df[sem_turma].copy()
    erros["alerta"] = "Aluno sem turma"
    erros["detalhe"] = "id_turma vazio"
    return erros