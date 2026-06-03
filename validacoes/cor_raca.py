import pandas as pd


VALORES_INVALIDOS = {"(vazio)", "Não declarada", "Nao declarada", "NAO DECLARADA"}


def checar_sem_autodeclaracao_racial(df: pd.DataFrame) -> pd.DataFrame:
    """
    Detecta alunos sem autodeclaração racial:
    - dc_cor_raca nulo (None/NaN)
    - dc_cor_raca vazio ("")
    - dc_cor_raca com valor em VALORES_INVALIDOS
    """
    if "dc_cor_raca" not in df.columns:
        return pd.DataFrame()

    col = df["dc_cor_raca"]

    mask = (
        col.isna()
        | (col.str.strip() == "")
        | col.isin(VALORES_INVALIDOS)
    )

    erros = df[mask].copy()
    erros["alerta"] = "Sem_autodeclaracao_racial"
    erros["detalhe"] = col[mask].apply(
        lambda v: "Campo vazio/nulo" if pd.isna(v) or str(v).strip() == ""
        else f"Valor: '{v}'"
    )
    return erros
