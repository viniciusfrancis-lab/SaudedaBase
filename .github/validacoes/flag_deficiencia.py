import pandas as pd


def checar_flag_deficiencia(df: pd.DataFrame) -> pd.DataFrame:
    """
    Valida a consistência entre fl_deficiencia e dc_deficiencia:
      - fl_deficiencia = 'Sim' → dc_deficiencia NÃO pode ser vazia
      - fl_deficiencia = 'Não' → dc_deficiencia DEVE ser vazia
    """
    if "fl_deficiencia" not in df.columns or "dc_deficiencia" not in df.columns:
        return pd.DataFrame()

    fl  = df["fl_deficiencia"].str.strip()
    dc  = df["dc_deficiencia"].fillna("").str.strip()

    # Regra 1: fl = Sim mas dc está vazia
    mask_sim_sem_dc = (fl == "Sim") & (dc == "")

    # Regra 2: fl = Não mas dc está preenchida
    mask_nao_com_dc = (fl == "Não") & (dc != "")

    erros_sim = df[mask_sim_sem_dc].copy()
    erros_sim["alerta"]  = "Deficiência sem descrição"
    erros_sim["detalhe"] = "fl_deficiencia=Sim mas dc_deficiencia está vazia"

    erros_nao = df[mask_nao_com_dc].copy()
    erros_nao["alerta"]  = "Descrição de deficiência indevida"
    erros_nao["detalhe"] = "fl_deficiencia=Não mas dc_deficiencia está preenchida"

    resultados = [e for e in [erros_sim, erros_nao] if not e.empty]
    return pd.concat(resultados, ignore_index=True) if resultados else pd.DataFrame()
