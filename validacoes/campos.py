<<<<<<< HEAD
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
=======
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
>>>>>>> c382a788aecb11f6e1ce22bd4da61dc34a177237
    return pd.concat(resultados, ignore_index=True) if resultados else pd.DataFrame()