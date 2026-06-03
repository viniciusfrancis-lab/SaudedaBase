from pathlib import Path
from datetime import date
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from alertas_config import ALERTA_ULTIMA_APARICAO

# ── Configurações ──────────────────────────────────────────────────────────────
PARQUET = Path(__file__).parent.parent / "dados" / "MATRICULAS_GERAIS.parquet"

COLUNAS_NECESSARIAS = [
    "nm_aluno",
    "id_aluno",
    "data_referencia",
    "num_ano_letivo",
    "situacao_enturmacao",
    "situacao_matricula",
    "data_encerramento_matricula",
    "data_saida",
    "nm_regional",
    "nm_municipio",
    "nm_escola",
]

COLUNAS_SAIDA = [
    "nm_regional",
    "nm_municipio",
    "nm_escola",
    "id_aluno",
    "nm_aluno",
    "ultima_aparicao",
    "situacao_enturmacao",
    "situacao_matricula",
]

# Diagnóstico: imprime histórico completo do aluno informado. Deixe "" para desativar.
DIAGNOSTICO_ALUNO = ""


# ── Filtro de ano letivo ───────────────────────────────────────────────────────
# ── Filtro de ano letivo ───────────────────────────────────────────────────────
def aplicar_filtros(df: pd.DataFrame) -> pd.DataFrame:
    """Apenas ano letivo 2026 (exceto MEPES). NÃO filtra por situação."""

    # Filtra anos 2026
    anos_2026 = [v for v in df["num_ano_letivo"].unique() if "2026" in str(v)]
    df = df[df["num_ano_letivo"].isin(anos_2026)].copy()
    
    # Remove MEPES
    df = df[df["num_ano_letivo"] != "2026 - MEPES"].copy()
    
    return df.reset_index(drop=True)


# ── Lógica principal ───────────────────────────────────────────────────────────
def gerar_alerta(verbose: bool = True) -> pd.DataFrame:
    """
    Gera DataFrame de alertos com uma linha por aluno único.
    
    LÓGICA:
    1. Carrega dados
    2. Para cada aluno, encontra MÁXIMA data_referencia (data_maxima)
    3. Filtra: mantém apenas alunos com data_maxima < hoje
    4. Para cada aluno filtrado: encontra última data "Em curso"
    5. Se última "Em curso" = data_maxima: ALERTA
    6. Se última "Em curso" ≠ data_maxima: IGNORA
    
    Saída: DataFrame deduplicated com colunas COLUNAS_SAIDA.
    """
    def _log(msg):
        if verbose:
            print(msg)

    hoje = pd.Timestamp.today().normalize()
    _log(f"Hoje: {hoje}")

    # 1. Descobre colunas disponíveis no parquet
    _log("Carregando parquet...")
    
    # ✅ OTIMIZAÇÃO: Lê metadados sem carregar dados
    schema = pq.read_schema(PARQUET)
    cols_disponiveis = schema.names
    
    cols_ler = [c for c in COLUNAS_NECESSARIAS if c in cols_disponiveis]
    ausentes = [c for c in COLUNAS_NECESSARIAS if c not in cols_disponiveis]
    if ausentes:
        _log(f"  ⚠ Colunas ausentes no parquet (ignoradas): {ausentes}")

    # 2. Lê o parquet COM FILTRO de ano letivo (economiza memória)
    try:
        # Filtro direto no PyArrow (mais eficiente)
        table = pq.read_table(
            PARQUET,
            columns=cols_ler,
            filters=[
                ('num_ano_letivo', 'in', [
                    '2026', '2026 - REGULAR', '2026 - EJA', '2026 - PROFISSIONALIZANTE'
                ])
            ]
        )

        # Converte para pandas com dtypes numpy nativos (não Arrow-backed).
        # Strings viram object, datas viram datetime64 — operações como sort
        # e groupby são mais rápidas e estáveis do que com tipos Arrow.
        df = table.to_pandas(types_mapper=None)

        _log(f"  {len(df):,} registros carregados (filtrado na leitura).")

    except Exception as e:
        # Fallback: leitura normal + filtro depois (também sem types Arrow)
        _log(f"  ⚠️  Filtro PyArrow falhou ({str(e)[:50]}...), usando método padrão...")
        df = pd.read_parquet(PARQUET, columns=cols_ler, engine='pyarrow')
        _log(f"  {len(df):,} registros carregados.")
    
    df.columns = df.columns.str.strip().str.lower()

    # 3. Aplica filtros adicionais (remove MEPES e converte tipos)
    df = aplicar_filtros(df)
    _log(f"  {len(df):,} registros após filtros completos.")

    # 4. Converte data_referencia para date
    df["data_referencia"] = pd.to_datetime(
    df["data_referencia"],
    errors="coerce"
)
    # ── Diagnóstico (opcional) ─────────────────────────────────────────────────
    if DIAGNOSTICO_ALUNO:
        mask_diag = df["nm_aluno"].str.upper().str.contains(DIAGNOSTICO_ALUNO.upper(), na=False)
        df_diag = df[mask_diag].sort_values("data_referencia")
        if df_diag.empty:
            _log(f"\n[DIAG] '{DIAGNOSTICO_ALUNO}' não encontrado.")
        else:
            cols_diag = [c for c in ["nm_aluno", "data_referencia", "situacao_enturmacao",
                                     "situacao_matricula", "data_saida"] if c in df_diag.columns]
            _log(f"\n[DIAG] Histórico de '{DIAGNOSTICO_ALUNO}':")
            _log(df_diag[cols_diag].drop_duplicates().to_string(index=False))
            _log(f"  → Data máxima: {df_diag['data_referencia'].max()}\n")
    # ──────────────────────────────────────────────────────────────────────────

    # ✅ LÓGICA CORRIGIDA:
    # 5. Para cada id_aluno, encontra a MÁXIMA data_referencia (data_maxima)
    max_por_aluno = (
        df.groupby("id_aluno", as_index=False)["data_referencia"]
        .max()
        .rename(columns={"data_referencia": "data_maxima"})
    )
    _log(f"  {len(max_por_aluno):,} alunos únicos encontrados.")

    # 6. FILTRA: Mantém apenas alunos cuja data_maxima < hoje
    max_por_aluno = max_por_aluno[max_por_aluno["data_maxima"] < hoje].copy()
    _log(f"  {len(max_por_aluno):,} alunos com data_maxima anterior a hoje.")

    if max_por_aluno.empty:
        _log("Nenhum aluno encontrado com esse critério.")
        return pd.DataFrame(columns=COLUNAS_SAIDA) 

    # 7. Filtra df para apenas alunos com data_maxima < hoje
    df_filtrado = df[df["id_aluno"].isin(max_por_aluno["id_aluno"])].copy()
    _log(f"  {len(df_filtrado):,} registros para alunos filtrados.")

 # 8. Filtra apenas registros "Em curso"
    mask_em_curso = (
        df_filtrado["situacao_enturmacao"]
            .fillna("")
            .astype(str)
            .str.strip()
            .eq("Em curso")
        &
        df_filtrado["situacao_matricula"]
            .fillna("")
            .astype(str)
            .str.strip()
            .eq("Em curso")
)

    df_em_curso = df_filtrado.loc[mask_em_curso].copy()

    # 9. Pega a última ocorrência por aluno.
    #    Antes: sort_values("data_referencia") + groupby().tail(1) — aloca um
    #    índice global do DataFrame inteiro (estourou a memória no malloc).
    #    Agora: idxmax pega só o índice da linha de máxima data por aluno
    #    em uma passada, sem mover o DataFrame.
    idx_ultima = df_em_curso.groupby("id_aluno")["data_referencia"].idxmax()
    df_ultima_em_curso = df_em_curso.loc[idx_ultima].reset_index(drop=True)

# 10. Adiciona data máxima de cada aluno
    df_ultima_em_curso = df_ultima_em_curso.merge(
        max_por_aluno[["id_aluno", "data_maxima"]],
        on="id_aluno",
        how="inner"
)

# 11. FILTRO CRÍTICO
    df_alerta = df_ultima_em_curso[
        df_ultima_em_curso["data_referencia"] == df_ultima_em_curso["data_maxima"]
    ].copy()

    # 11b. Renomeia data_referencia → ultima_aparicao (nome esperado em COLUNAS_SAIDA)
    df_alerta = df_alerta.rename(columns={"data_referencia": "ultima_aparicao"})

    # 12. Deduplicação por id_aluno
    df_alerta = (
        df_alerta
        .sort_values("nm_escola", na_position="last")
        .drop_duplicates(subset=["id_aluno"], keep="last")
        .reset_index(drop=True)
    )
    _log(f"  {len(df_alerta):,} alunos únicos (APÓS deduplicação).")
    
    # Validação
    assert df_alerta["id_aluno"].nunique() == len(df_alerta), \
        "ERRO: deduplicação falhou! Ainda há id_aluno duplicados."
    _log(f"  ✅ Validação: 1 linha por id_aluno confirmado.")

    # 13. Seleciona e ordena colunas disponíveis
    cols_saida_ok = [c for c in COLUNAS_SAIDA if c in df_alerta.columns]
    sort_cols = [c for c in ["nm_regional", "nm_municipio", "nm_escola", "nm_aluno"]
                 if c in cols_saida_ok]
    df_final = df_alerta[cols_saida_ok].sort_values(sort_cols).reset_index(drop=True)

    return df_final


# ── Wrapper para o pipeline (processar.py) ────────────────────────────────────
def checar_ultima_aparicao(df_regras_auditoria: pd.DataFrame = None) -> pd.DataFrame:
    """
    Wrapper compatível com o pipeline do processar.py.
    
    ⚠️ IMPORTANTE: Este alerta DEVE RODAR POR ÚLTIMO!
    
    Args:
        df_regras_auditoria: DataFrame contendo APENAS as 6 regras de auditoria
    
    Retorna DataFrame com colunas padrão das validações.
    Garantia: uma linha por aluno único (deduplicated).
    """
    df = gerar_alerta(verbose=False)
    if df.empty:
        return pd.DataFrame()

    df = df.copy()
    
    # Filtro crítico: Excluir APENAS alunos das 6 regras de auditoria
    if df_regras_auditoria is not None and not df_regras_auditoria.empty:
        if "id_aluno" not in df_regras_auditoria.columns:
            print("  ⚠️  df_regras_auditoria sem coluna 'id_aluno'. Pulando filtro.")
        else:
            ids_regras = set(df_regras_auditoria["id_aluno"].dropna().unique())
            qtd_antes = len(df)
            df = df[~df["id_aluno"].isin(ids_regras)].copy()
            qtd_depois = len(df)
            qtd_excluidos = qtd_antes - qtd_depois
            
            if qtd_excluidos > 0:
                print(f"  ⚠️  Excluídos {qtd_excluidos:,} alunos que aparecem nas regras de auditoria.")
            
            if df.empty:
                print("  ℹ️  Nenhum alerta de 'última aparição' após filtrar regras de auditoria.")
                return pd.DataFrame()
    
    # Validar que ainda temos uma linha por aluno
    assert df["id_aluno"].nunique() == len(df), \
        f"ERRO em checar_ultima_aparicao(): perdi deduplicação! {df['id_aluno'].nunique()} únicos vs {len(df)} linhas"
    
    df["alerta"] = ALERTA_ULTIMA_APARICAO
    df["detalhe"] = (
        "Última aparição em " + df["ultima_aparicao"].astype(str)
        + " (ainda Em curso nessa data, depois desapareceu)"
    )
    
    return df