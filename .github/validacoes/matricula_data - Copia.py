# validacoes/matricula_data.py (VERSÃO POLARS)
"""
Auditoria temporal de matrículas — versão integrada ao Painel SEDU.

ARQUITETURA:
  ✅ Leitura e filtros com Polars (paralelo, lazy)
  ✅ Todas as regras de auditoria vetorizadas com Polars (sem loops Python)
  ✅ Regra 1: Matrícula retroativa
  ✅ Regra 2: Frequência io-iô
  ✅ Regra 3A: Mudança de id_aluno interna
  ✅ Regra 3B: Unificação externa de ID (join por nasc + similaridade)
  ✅ Regra 4: Alteração em dt_matricula
  ✅ Saída em pandas para compatibilidade com app.py e processar.py
"""
import io
import os
import difflib
from datetime import datetime, date
from pathlib import Path

import polars as pl
import pandas as pd
import streamlit as st

from alertas_config import (
    ALERTA_DT_MATRICULA,
    ALERTA_RETROATIVA,
    ALERTA_MUDANCA_ID,
    ALERTA_IOIO,
)

# ─── Configurações ────────────────────────────────────────────────────────────
INICIO_MARCO   = date(2026, 3, 1)
FIM_RETROATIVA = date(2026, 2, 15)

CAMINHO_PARQUET = Path(__file__).parent.parent / "dados" / "EMPILHADO_MATRICULAS.parquet"

COLUNAS_ALVO = [
    "id_aluno", "nm_aluno", "data_nascimento",
    "inep_escola", "nm_escola", "nm_regional", "nm_municipio",
    "nome_turma", "nome_ano_escolaridade",
    "id_ano", "id_ano_letivo", "num_ano_letivo",
    "dt_matricula", "dt_enturmacao",
    "situacao_enturmacao", "situacao_matricula", "tipo_atendimento",
    "cpf", "data_referencia",
    "nm_filiacao_1", "nm_filiacao_2",  # ✅ nomes corretos no parquet
]

COLUNAS_EXCEL = [
    "Regional", "Escola", "Nome_Estudante", "Data_Nascimento",
    "Matricula_Retroativa", "Detalhe_Retroativa",
    "IDs", "Status_ID", "Detalhe_Mudanca_ID",
    "Status_Matricula", "Detalhe_Mudanca_Matricula",
    "Status_Frequencia", "Detalhe_Frequencia",
    "Primeira_Aparicao", "Ultima_Aparicao", "Total_Semanas",
]


# =============================================================================
# 1. LEITURA E FILTROS (POLARS LAZY)
# =============================================================================
def _carregar_e_filtrar(verbose: bool = False) -> pl.DataFrame:
    """
    Lê EMPILHADO_MATRICULAS.parquet via Polars lazy e aplica os mesmos filtros
    do carregador.py:
      1. num_ano_letivo contém "2026" E não contém "MEPES"
      2. situacao_matricula == "Em curso"
      3. situacao_enturmacao == "Em curso"
      4. data_encerramento_matricula é nula
      5. tipo_atendimento == "Regular"

    literal=False garante que \\s+ seja tratado como regex em todas as versões Polars.
    """
    def _log(msg):
        st.info(msg) if verbose else print(f"  [matricula_data] {msg}")

    if not CAMINHO_PARQUET.exists():
        _log(f"❌ Arquivo não encontrado: {CAMINHO_PARQUET}")
        return pl.DataFrame()

    schema = pl.read_parquet_schema(CAMINHO_PARQUET)

    # Inclui data_encerramento_matricula se existir no parquet
    colunas_extras = ["data_encerramento_matricula"]
    cols_ler = [c for c in COLUNAS_ALVO + colunas_extras if c in schema]
    _log(f"Colunas: {len(cols_ler)} disponíveis no parquet")

    lf = pl.scan_parquet(CAMINHO_PARQUET).select(cols_ler)

    # Log do total bruto antes de qualquer filtro
    n_total = lf.select(pl.len()).collect().item()
    _log(f"Total bruto: {n_total:,} linhas")

    # ── Filtro 1: Ano letivo 2026, sem MEPES ─────────────────────────────────
    if "num_ano_letivo" in cols_ler:
        col_ano = pl.col("num_ano_letivo").cast(pl.Utf8)
        lf = lf.filter(col_ano.str.contains("2026") & ~col_ano.str.contains("MEPES"))

    # ── Filtro 2: Matrícula Em curso ─────────────────────────────────────────
    if "situacao_matricula" in cols_ler:
        lf = lf.filter(
            pl.col("situacao_matricula").cast(pl.Utf8)
              .str.to_lowercase()
              .str.replace_all(r"\s+", "", literal=False) == "emcurso"
        )

    # ── Filtro 3: Enturmação Em curso ────────────────────────────────────────
    if "situacao_enturmacao" in cols_ler:
        lf = lf.filter(
            pl.col("situacao_enturmacao").cast(pl.Utf8)
              .str.to_lowercase()
              .str.replace_all(r"\s+", "", literal=False) == "emcurso"
        )

    # ── Filtro 4: Sem data de encerramento (mesmo critério do carregador.py) ─
    if "data_encerramento_matricula" in cols_ler:
        lf = lf.filter(pl.col("data_encerramento_matricula").is_null())

    # ── Filtro 5: Atendimento Regular ────────────────────────────────────────
    if "tipo_atendimento" in cols_ler:
        lf = lf.filter(
            pl.col("tipo_atendimento").cast(pl.Utf8)
              .str.to_lowercase()
              .str.replace_all(r"\s+", "", literal=False) == "regular"
        )

    df = lf.collect()
    _log(f"✅ {len(df):,} linhas após filtros (2026 s/ MEPES, Em curso, Regular, s/ encerramento)")
    return df


# =============================================================================
# 2. NORMALIZAÇÃO DE CAMPOS TEMPORAIS
# =============================================================================
def _preparar_df(df: pl.DataFrame) -> pl.DataFrame:
    """Normaliza campos, parseia datas, adiciona coluna 'semana'."""

    # Colunas opcionais de filiação
    if "nm_filiacao_1" not in df.columns:
        df = df.with_columns(pl.lit("").alias("nm_filiacao_1"))
    if "nm_filiacao_2" not in df.columns:
        df = df.with_columns(pl.lit("").alias("nm_filiacao_2"))

    df = df.with_columns([
        pl.col("id_aluno").cast(pl.Utf8).str.replace(r"\.0$", "").str.strip_chars(),
        pl.col("nm_aluno").cast(pl.Utf8).str.to_uppercase().str.strip_chars(),
        pl.col("nm_filiacao_1").cast(pl.Utf8).str.to_uppercase().str.strip_chars().fill_null(""),
        pl.col("nm_filiacao_2").cast(pl.Utf8).str.to_uppercase().str.strip_chars().fill_null(""),
    ])

    # semana: data_referencia → Date
    df = df.with_columns(
        pl.col("data_referencia").cast(pl.Utf8).str.slice(0, 10)
          .str.to_date(format="%Y-%m-%d", strict=False).alias("semana")
    )

    # dt_matricula → Date
    df = df.with_columns(
        pl.col("dt_matricula").cast(pl.Utf8).str.slice(0, 10)
          .str.to_date(format="%Y-%m-%d", strict=False).alias("dt_matricula_dt")
    )

    # nasc_str: string DD/MM/YYYY para usar como chave de agrupamento
    df = df.with_columns(
        pl.col("data_nascimento").cast(pl.Utf8).str.slice(0, 10)
          .str.to_date(format="%Y-%m-%d", strict=False)
          .dt.strftime("%d/%m/%Y").fill_null("DATA_INVALIDA").alias("nasc_str")
    )

    return df.filter(pl.col("semana").is_not_null())


# =============================================================================
# REGRA 1 — MATRÍCULA RETROATIVA
# =============================================================================
def _regra_matricula_retroativa(df: pl.DataFrame) -> pl.DataFrame:
    primeira = (
        df.group_by(["nm_aluno", "nasc_str"])
          .agg(pl.col("semana").min().alias("primeira_semana"))
    )

    df_r1 = (
        df.join(primeira, on=["nm_aluno", "nasc_str"])
          .filter(pl.col("semana") == pl.col("primeira_semana"))
          .filter(pl.col("dt_matricula_dt").is_not_null())
          .group_by(["nm_aluno", "nasc_str", "primeira_semana"])
          .agg(pl.col("dt_matricula_dt").min().alias("min_dt_mat"))
          .filter(
              (pl.col("primeira_semana") >= pl.lit(INICIO_MARCO))
              & (pl.col("min_dt_mat") <= pl.lit(FIM_RETROATIVA))
          )
          .with_columns([
              pl.lit("Sim").alias("Matricula_Retroativa"),
              (
                  pl.lit("Apareceu em ")
                  + pl.col("primeira_semana").dt.strftime("%d/%m/%Y")
                  + pl.lit(" com matrícula retroativa de ")
                  + pl.col("min_dt_mat").dt.strftime("%d/%m/%Y")
              ).alias("Detalhe_Retroativa"),
          ])
          .select(["nm_aluno", "nasc_str", "Matricula_Retroativa", "Detalhe_Retroativa"])
    )
    return df_r1


# =============================================================================
# REGRA 2 — FREQUÊNCIA IO-IÔ
# =============================================================================
def _regra_ioio(df: pl.DataFrame, datas_semanas: list) -> pl.DataFrame:
    pos_map = {d: i for i, d in enumerate(datas_semanas)}

    por_aluno = (
        df.group_by(["nm_aluno", "nasc_str"])
          .agg(pl.col("semana").unique().sort().alias("semanas_aluno"))
    )

    registros = []
    for row in por_aluno.iter_rows(named=True):
        sems = row["semanas_aluno"]
        if len(sems) < 2:
            continue
        gaps = []
        for j in range(len(sems) - 1):
            pa = pos_map.get(sems[j])
            pb = pos_map.get(sems[j + 1])
            if pa is None or pb is None:
                continue
            if pb > pa + 1:
                faltantes = [datas_semanas[k].strftime("%d/%m/%Y") for k in range(pa + 1, pb)]
                gaps.append(
                    f"Ausente entre {sems[j].strftime('%d/%m/%Y')} "
                    f"e {sems[j+1].strftime('%d/%m/%Y')} "
                    f"(Faltou em: {', '.join(faltantes)})"
                )
        if gaps:
            registros.append({
                "nm_aluno": row["nm_aluno"],
                "nasc_str": row["nasc_str"],
                "Status_Frequencia": "Io-iô",
                "Detalhe_Frequencia": " | ".join(gaps),
            })

    if not registros:
        return pl.DataFrame(schema={
            "nm_aluno": pl.Utf8, "nasc_str": pl.Utf8,
            "Status_Frequencia": pl.Utf8, "Detalhe_Frequencia": pl.Utf8,
        })
    return pl.DataFrame(registros)


# =============================================================================
# REGRA 3A — MUDANÇA DE ID INTERNA
# =============================================================================
def _regra_mudanca_id(df: pl.DataFrame) -> pl.DataFrame:
    # IDs únicos por aluno por semana — vetorizado
    ids_semana = (
        df.group_by(["nm_aluno", "nasc_str", "semana"])
          .agg(pl.col("id_aluno").unique().sort().str.join(",").alias("ids_semana"))
          .sort(["nm_aluno", "nasc_str", "semana"])
    )

    # Conta IDs distintos por aluno
    ids_count = (
        df.group_by(["nm_aluno", "nasc_str"])
          .agg(pl.col("id_aluno").n_unique().alias("n_ids"),
               pl.col("id_aluno").unique().sort().str.join(", ").alias("todos_ids"))
    )

    # Só quem tem mais de 1 ID
    multi = ids_count.filter(pl.col("n_ids") > 1)
    if multi.is_empty():
        return pl.DataFrame(schema={
            "nm_aluno": pl.Utf8, "nasc_str": pl.Utf8,
            "todos_ids": pl.Utf8, "Status_ID": pl.Utf8, "Detalhe_Mudanca_ID": pl.Utf8,
        })

    registros = []
    for aluno in multi.iter_rows(named=True):
        sems = (
            ids_semana.filter(
                (pl.col("nm_aluno") == aluno["nm_aluno"])
                & (pl.col("nasc_str") == aluno["nasc_str"])
            ).to_dicts()
        )
        mudancas = []
        for j in range(1, len(sems)):
            if sems[j]["ids_semana"] != sems[j-1]["ids_semana"]:
                mudancas.append(
                    f"Em {sems[j]['semana'].strftime('%d/%m/%Y')} "
                    f"mudou de {sems[j-1]['ids_semana']} "
                    f"para {sems[j]['ids_semana']}"
                )
        registros.append({
            "nm_aluno": aluno["nm_aluno"],
            "nasc_str": aluno["nasc_str"],
            "todos_ids": aluno["todos_ids"],
            "Status_ID": "Alterado",
            "Detalhe_Mudanca_ID": " | ".join(mudancas),
        })

    return pl.DataFrame(registros)


# =============================================================================
# REGRA 3B — UNIFICAÇÃO EXTERNA DE ID
# =============================================================================
def _regra_unificacao_id(df: pl.DataFrame, datas_semanas: list) -> pl.DataFrame:
    """
    Compara apenas alunos com mesma data de nascimento.
    Para cada par (nasc igual, nome/mãe/pai ≥90% similar) que aparece
    em semanas consecutivas, registra unificação.
    """
    # pos_map com chave STRING para evitar problemas de tipo date vs date
    pos_map = {d.strftime("%Y-%m-%d"): i for i, d in enumerate(datas_semanas)}

    def _safe(v):
        return "" if not v or str(v).lower() in ("nan", "none", "") else str(v).strip()

    def _to_key(d) -> str:
        """Converte qualquer representação de data para string YYYY-MM-DD."""
        if d is None:
            return ""
        if hasattr(d, "strftime"):
            return d.strftime("%Y-%m-%d")
        return str(d)[:10]

    perfis = (
        df.group_by(["nm_aluno", "nasc_str"])
          .agg([
              pl.col("semana").min().alias("primeira_semana"),
              pl.col("semana").max().alias("ultima_semana"),
              pl.col("id_aluno").unique().sort().str.join(", ").alias("todos_ids"),
              pl.col("nm_filiacao_1").first().alias("nm_filiacao_1"),
              pl.col("nm_filiacao_2").first().alias("nm_filiacao_2"),
              pl.col("nm_escola").first().alias("nm_escola"),
              pl.col("nm_regional").first().alias("nm_regional"),
          ])
    )

    por_nasc: dict = {}
    for row in perfis.iter_rows(named=True):
        por_nasc.setdefault(row["nasc_str"], []).append(row)

    n_nome_ok = 0
    n_mae_pai_ok = 0
    n_consecutivo_ok = 0

    registros = []
    for nasc, grupo in por_nasc.items():
        if len(grupo) < 2:
            continue
        for i in range(len(grupo)):
            for j in range(i + 1, len(grupo)):
                p, outro = grupo[i], grupo[j]
                nome_p = _safe(p["nm_aluno"])
                nome_o = _safe(outro["nm_aluno"])
                if not nome_p or not nome_o:
                    continue
                sim_nome = difflib.SequenceMatcher(None, nome_p, nome_o).ratio()
                if sim_nome < 0.90:
                    continue
                n_nome_ok += 1

                mae_p = _safe(p["nm_filiacao_1"]); mae_o = _safe(outro["nm_filiacao_1"])
                pai_p = _safe(p["nm_filiacao_2"]); pai_o = _safe(outro["nm_filiacao_2"])
                sim_mae = difflib.SequenceMatcher(None, mae_p, mae_o).ratio() if mae_p and mae_o else 0.0
                sim_pai = difflib.SequenceMatcher(None, pai_p, pai_o).ratio() if pai_p and pai_o else 0.0
                # Lógica original: nome E filiacao_1 E filiacao_2 todos ≥90%
                if not (sim_nome >= 0.90 and sim_mae >= 0.90 and sim_pai >= 0.90):
                    continue
                n_mae_pai_ok += 1

                for antigo, novo in [(p, outro), (outro, p)]:
                    ult_key = _to_key(antigo["ultima_semana"])
                    pri_key = _to_key(novo["primeira_semana"])
                    if ult_key not in pos_map or pri_key not in pos_map:
                        continue
                    if pos_map[pri_key] == pos_map[ult_key] + 1:
                        n_consecutivo_ok += 1
                        ult = antigo["ultima_semana"]
                        pri = novo["primeira_semana"]
                        registros.append({
                            "nm_aluno": novo["nm_aluno"],
                            "nasc_str": novo["nasc_str"],
                            "ids_unificados": antigo["todos_ids"] + ", " + novo["todos_ids"],
                            "Detalhe_Unificacao": (
                                f"UNIFICAÇÃO: ID {antigo['todos_ids']} "
                                f"(até {_to_key(ult)}) "
                                f"-> ID {novo['todos_ids']} "
                                f"(desde {_to_key(pri)})"
                            ),
                        })

    print(f"  [matricula_data] R3B funil: nome≥90%={n_nome_ok} | +mae/pai≥90%={n_mae_pai_ok} | +consecutivo={n_consecutivo_ok}")

    if not registros:
        return pl.DataFrame(schema={
            "nm_aluno": pl.Utf8, "nasc_str": pl.Utf8,
            "ids_unificados": pl.Utf8, "Detalhe_Unificacao": pl.Utf8,
        })
    return pl.DataFrame(registros)


# =============================================================================
# REGRA 4 — ALTERAÇÃO EM DT_MATRICULA
# =============================================================================
def _regra_dt_matricula(df: pl.DataFrame) -> pl.DataFrame:
    mat_semana = (
        df.filter(pl.col("dt_matricula_dt").is_not_null())
          .group_by(["nm_aluno", "nasc_str", "semana"])
          .agg(pl.col("dt_matricula_dt").min().alias("min_dt_mat"))
          .sort(["nm_aluno", "nasc_str", "semana"])
    )

    alunos_multi = (
        mat_semana.group_by(["nm_aluno", "nasc_str"])
                  .agg(pl.col("semana").count().alias("n_sem"))
                  .filter(pl.col("n_sem") > 1)
    )
    if alunos_multi.is_empty():
        return pl.DataFrame(schema={
            "nm_aluno": pl.Utf8, "nasc_str": pl.Utf8,
            "Status_Matricula": pl.Utf8, "Detalhe_Mudanca_Matricula": pl.Utf8,
        })

    registros = []
    for aluno in alunos_multi.iter_rows(named=True):
        sems = (
            mat_semana.filter(
                (pl.col("nm_aluno") == aluno["nm_aluno"])
                & (pl.col("nasc_str") == aluno["nasc_str"])
            ).to_dicts()
        )
        status, detalhe = "Ok", ""
        for j in range(1, len(sems)):
            d_ant = sems[j-1]["min_dt_mat"]
            d_atu = sems[j]["min_dt_mat"]
            if d_ant and d_atu and d_ant != d_atu:
                if d_atu < d_ant:
                    status = "ALERTA: Retrocedeu"
                elif status == "Ok":
                    status = "Alterada (Avançou)"
                detalhe = (
                    f"Em {sems[j]['semana'].strftime('%d/%m/%Y')} passou de "
                    f"{d_ant.strftime('%d/%m/%Y')} para {d_atu.strftime('%d/%m/%Y')}"
                )
        if status != "Ok":
            registros.append({
                "nm_aluno": aluno["nm_aluno"],
                "nasc_str": aluno["nasc_str"],
                "Status_Matricula": status,
                "Detalhe_Mudanca_Matricula": detalhe,
            })

    if not registros:
        return pl.DataFrame(schema={
            "nm_aluno": pl.Utf8, "nasc_str": pl.Utf8,
            "Status_Matricula": pl.Utf8, "Detalhe_Mudanca_Matricula": pl.Utf8,
        })
    return pl.DataFrame(registros)


# =============================================================================
# CONSOLIDAÇÃO WIDE
# =============================================================================
def _consolidar_wide(
    df: pl.DataFrame,
    r1: pl.DataFrame, r2: pl.DataFrame,
    r3a: pl.DataFrame, r3b: pl.DataFrame,
    r4: pl.DataFrame,
) -> pl.DataFrame:

    base = (
        df.group_by(["nm_aluno", "nasc_str"])
          .agg([
              pl.col("nm_escola").first().alias("Escola"),
              pl.col("nm_regional").first().alias("Regional"),
              pl.col("id_aluno").unique().sort().str.join(", ").alias("IDs"),
              pl.col("semana").min().dt.strftime("%d/%m/%Y").alias("Primeira_Aparicao"),
              pl.col("semana").max().dt.strftime("%d/%m/%Y").alias("Ultima_Aparicao"),
              pl.col("semana").n_unique().alias("Total_Semanas"),
          ])
    )

    wide = base

    # R1
    wide = wide.join(r1, on=["nm_aluno", "nasc_str"], how="left").with_columns([
        pl.col("Matricula_Retroativa").fill_null("Não"),
        pl.col("Detalhe_Retroativa").fill_null(""),
    ])

    # R2
    wide = wide.join(r2, on=["nm_aluno", "nasc_str"], how="left").with_columns([
        pl.col("Status_Frequencia").fill_null("Regular"),
        pl.col("Detalhe_Frequencia").fill_null(""),
    ])

    # R3A
    wide = wide.join(r3a, on=["nm_aluno", "nasc_str"], how="left").with_columns([
        pl.col("Status_ID").fill_null("Único"),
        pl.col("Detalhe_Mudanca_ID").fill_null(""),
        pl.col("todos_ids").fill_null(""),
    ])

    # R3B — garante que alunos detectados APENAS por unificação entrem no wide
    # antes do join, marca quem a R3B encontrou
    if not r3b.is_empty():
        r3b_marcado = r3b.with_columns(pl.lit(True).alias("_tem_unificacao"))

        # Alunos da R3B que ainda NÃO estão no wide (só detectados por unificação)
        novos_r3b = (
            r3b_marcado
            .join(wide.select(["nm_aluno", "nasc_str"]), on=["nm_aluno", "nasc_str"], how="anti")
            .join(base, on=["nm_aluno", "nasc_str"], how="left")
            .with_columns([
                pl.lit("Não").alias("Matricula_Retroativa"),
                pl.lit("").alias("Detalhe_Retroativa"),
                pl.lit("Regular").alias("Status_Frequencia"),
                pl.lit("").alias("Detalhe_Frequencia"),
                pl.lit("Alterado").alias("Status_ID"),
                pl.col("Detalhe_Unificacao").fill_null("").alias("Detalhe_Mudanca_ID"),
                pl.col("ids_unificados").fill_null("").alias("todos_ids"),
                pl.lit("Ok").alias("Status_Matricula"),
                pl.lit("").alias("Detalhe_Mudanca_Matricula"),
                pl.col("ids_unificados").fill_null(""),
                pl.col("Detalhe_Unificacao").fill_null(""),
            ])
        )

        # Adiciona novos ao wide antes do join normal da R3B
        if not novos_r3b.is_empty():
            cols_wide = wide.columns
            novos_alinhado = novos_r3b.select([c for c in cols_wide if c in novos_r3b.columns])
            wide = pl.concat([wide, novos_alinhado], how="diagonal")

        # Agora faz o join normal para enriquecer quem já estava no wide
        wide = wide.join(r3b_marcado, on=["nm_aluno", "nasc_str"], how="left").with_columns([
            pl.when(pl.col("_tem_unificacao").fill_null(False))
              .then(pl.lit("Alterado")).otherwise(pl.col("Status_ID")).alias("Status_ID"),
            pl.when(pl.col("_tem_unificacao").fill_null(False) & (pl.col("Detalhe_Mudanca_ID") == ""))
              .then(pl.col("Detalhe_Unificacao").fill_null(""))
              .when(pl.col("_tem_unificacao").fill_null(False))
              .then(pl.col("Detalhe_Mudanca_ID") + pl.lit(" | ") + pl.col("Detalhe_Unificacao").fill_null(""))
              .otherwise(pl.col("Detalhe_Mudanca_ID")).alias("Detalhe_Mudanca_ID"),
            pl.col("Detalhe_Unificacao").fill_null(""),
            pl.col("ids_unificados").fill_null(""),
        ])
    else:
        wide = wide.with_columns([
            pl.lit("").alias("Detalhe_Unificacao"),
            pl.lit("").alias("ids_unificados"),
        ])

    # R4
    wide = wide.join(r4, on=["nm_aluno", "nasc_str"], how="left").with_columns([
        pl.col("Status_Matricula").fill_null("Ok"),
        pl.col("Detalhe_Mudanca_Matricula").fill_null(""),
    ])

    # Só quem tem alerta
    wide = wide.filter(
        (pl.col("Matricula_Retroativa") == "Sim")
        | (pl.col("Status_Frequencia") == "Io-iô")
        | (pl.col("Status_ID") == "Alterado")
        | (pl.col("Status_Matricula") != "Ok")
    )

    wide = wide.rename({"nm_aluno": "Nome_Estudante", "nasc_str": "Data_Nascimento"})

    cols_ok = [c for c in COLUNAS_EXCEL if c in wide.columns]
    return wide.select(cols_ok)


# =============================================================================
# WIDE → LONG (pandas)
# =============================================================================
def _wide_para_long(df_wide: pd.DataFrame) -> pd.DataFrame:
    if df_wide.empty:
        return pd.DataFrame()
    linhas = []
    for _, r in df_wide.iterrows():
        base = {
            "nm_regional":       r.get("Regional", ""),
            "nm_escola":         r.get("Escola", ""),
            "nm_aluno":          r.get("Nome_Estudante", ""),
            "id_aluno":          r.get("IDs", ""),
            "data_nascimento":   r.get("Data_Nascimento", ""),
            "primeira_aparicao": r.get("Primeira_Aparicao", ""),
            "ultima_aparicao":   r.get("Ultima_Aparicao", ""),
            "total_semanas":     r.get("Total_Semanas", ""),
        }
        if r.get("Status_Matricula", "Ok") != "Ok":
            linhas.append({**base, "alerta": ALERTA_DT_MATRICULA,
                           "detalhe": f"{r['Status_Matricula']} — {r['Detalhe_Mudanca_Matricula']}"})
        if r.get("Matricula_Retroativa", "Não") == "Sim":
            linhas.append({**base, "alerta": ALERTA_RETROATIVA,
                           "detalhe": r["Detalhe_Retroativa"]})
        if r.get("Status_ID", "Único") == "Alterado":
            linhas.append({**base, "alerta": ALERTA_MUDANCA_ID,
                           "detalhe": r["Detalhe_Mudanca_ID"]})
        if r.get("Status_Frequencia", "Regular") == "Io-iô":
            linhas.append({**base, "alerta": ALERTA_IOIO,
                           "detalhe": r["Detalhe_Frequencia"]})
    return pd.DataFrame(linhas)


# =============================================================================
# EXCEL
# =============================================================================
def _gerar_excel_bytes(df_wide: pd.DataFrame) -> bytes:
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        if df_wide.empty:
            pd.DataFrame({"Aviso": ["Nenhuma inconsistência encontrada."]}) \
              .to_excel(writer, sheet_name="Alertas_Data_Quality", index=False)
        else:
            df_wide.to_excel(writer, sheet_name="Alertas_Data_Quality", index=False)
    buf.seek(0)
    return buf.getvalue()


# =============================================================================
# PIPELINE PRINCIPAL
# =============================================================================
def _executar_pipeline(verbose: bool = False) -> tuple[pd.DataFrame, pd.DataFrame]:
    def _log(msg): st.info(msg)     if verbose else print(f"  [matricula_data] {msg}")
    def _ok(msg):  st.success(msg)  if verbose else print(f"  [matricula_data] ✅ {msg}")
    def _err(msg): st.error(msg)    if verbose else print(f"  [matricula_data] ❌ {msg}")

    _log("Carregando parquet (Polars lazy)...")
    df = _carregar_e_filtrar(verbose)
    if df.is_empty():
        _err("Nenhum dado carregado.")
        return pd.DataFrame(), pd.DataFrame()

    _log("Normalizando campos...")
    df = _preparar_df(df)
    if df.is_empty():
        _err("Nenhum registro com semana válida.")
        return pd.DataFrame(), pd.DataFrame()

    datas_semanas = sorted(df["semana"].unique().to_list())
    n_alunos = df.select(["nm_aluno", "nasc_str"]).unique().height
    _ok(f"{n_alunos:,} alunos | {len(datas_semanas)} semanas")

    _log("Regra 1: Matrícula retroativa...")
    r1 = _regra_matricula_retroativa(df)

    _log("Regra 2: Frequência io-iô...")
    r2 = _regra_ioio(df, datas_semanas)

    _log("Regra 3A: Mudança de id_aluno...")
    r3a = _regra_mudanca_id(df)

    _log("Regra 3B: Unificação de ID (mesmo nasc)...")
    r3b = _regra_unificacao_id(df, datas_semanas)
    if not verbose:
        # Diagnóstico: verifica se nm_mae/nm_pai existem e quantos grupos têm ≥2 pessoas
        tem_mae = "nm_filiacao_1" in df.columns and df["nm_filiacao_1"].drop_nulls().len() > 0
        tem_pai = "nm_filiacao_2" in df.columns and df["nm_filiacao_2"].drop_nulls().len() > 0
        grupos_multi = sum(1 for v in df.group_by("nasc_str").agg(pl.col("nm_aluno").n_unique().alias("n"))["n"].to_list() if v >= 2)
        print(f"  [matricula_data] R3B encontrou {len(r3b):,} caso(s) de unificação.")
        print(f"  [matricula_data] R3B diagnóstico: nm_filiacao_1={'✅' if tem_mae else '❌ AUSENTE'} | nm_filiacao_2={'✅' if tem_pai else '❌ AUSENTE'} | grupos_nasc_multi={grupos_multi:,}")

    _log("Regra 4: Alteração em dt_matricula...")
    r4 = _regra_dt_matricula(df)

    _log("Consolidando resultados...")
    df_wide = _consolidar_wide(df, r1, r2, r3a, r3b, r4).to_pandas()

    if df_wide.empty:
        _ok(f"Nenhuma inconsistência em {n_alunos:,} alunos.")
        return df_wide, pd.DataFrame()

    _ok(f"{len(df_wide):,} aluno(s) com inconsistência.")
    df_long = _wide_para_long(df_wide)
    _ok(f"{len(df_long):,} alerta(s) individuais.")
    return df_wide, df_long


# =============================================================================
# FUNÇÕES PÚBLICAS
# =============================================================================
def checar_alteracao_dt_matricula() -> pd.DataFrame:
    """CLI — chamada por processar.py. Retorna DataFrame longo."""
    _, df_long = _executar_pipeline(verbose=False)
    return df_long


def checar_alteracao_dt_matricula_ui() -> pd.DataFrame:
    """UI Streamlit — chamada por app.py. Exibe progresso e download."""
    st.markdown("---")
    st.markdown("#### 📅 Auditoria Temporal — Análise de Inconsistências")
    with st.spinner("🔄 Executando auditoria com Polars..."):
        df_wide, df_long = _executar_pipeline(verbose=True)

    if df_wide.empty:
        st.success("✅ Nenhuma inconsistência encontrada.")
    st.download_button(
        "📥 Baixar Excel",
        _gerar_excel_bytes(df_wide),
        "Auditoria_Temporal.xlsx",
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        key="dl_auditoria",
    )
    if not df_wide.empty:
        with st.expander("👁 Prévia (primeiros 50 registros)", expanded=False):
            st.dataframe(df_wide.head(50), use_container_width=True, hide_index=True)
    return df_long
