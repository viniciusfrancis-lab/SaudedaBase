import pandas as pd
 
CHAVES_DUPLICATA = [
    "id_aluno",
    "nm_escola",
    "nome_ano_escolaridade",
    "ano_escolaridade",
    "nome_turma",
    "dc_turno",
]
 
def checar_duplicatas(df: pd.DataFrame) -> pd.DataFrame:
    # Usa apenas as colunas que existem na base
    subset = [c for c in CHAVES_DUPLICATA if c in df.columns]
 
    duplicados = df[df.duplicated(subset=subset, keep=False)].copy()
    duplicados["alerta"] = "Matrícula duplicada"
    duplicados["detalhe"] = (
        "id_aluno aparece mais de uma vez com mesma escola, "
        "ano de escolaridade, turma e turno"
    )
    return duplicados
 