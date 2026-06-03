import pandas as pd

CAMPOS = ["nm_aluno", "data_nascimento", "id_aluno"]

def checar_campos_vazios(df: pd.DataFrame) -> pd.DataFrame:
    resultados = []
    for campo in CAMPOS:
        if campo not in df.columns:
            continue
        vazios = df[df[campo].isna() | (df[campo].str.strip() == "")].copy()
        vazios["alerta"] = "Campo obrigatório vazio"
        vazios["detalhe"] = f"Campo '{campo}' vazio"
        resultados.append(vazios)
    return pd.concat(resultados, ignore_index=True) if resultados else pd.DataFrame()