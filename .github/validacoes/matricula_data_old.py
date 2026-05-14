# validacoes/matricula_data.py (VERSÃO REFATORADA)
"""
Auditoria temporal de matrículas — versão integrada ao Painel SEDU.

MUDANÇAS IMPLEMENTADAS:
  ✅ Leitura direta de EMPILHADO_MATRICULAS.parquet
  ✅ Implementação completa da Regra 6: Deficiência Cruzada
  ✅ 6 regras de auditoria temporal funcionando
  ✅ Saída em DataFrame wide (1 linha/aluno) + Excel com 2 abas
"""
import io
import os
from datetime import datetime
from pathlib import Path
import pandas as pd
import streamlit as st

from alertas_config import (
    ALERTA_DT_MATRICULA,
    ALERTA_RETROATIVA,    
    ALERTA_MUDANCA_ID,
    ALERTA_IOIO,
    
)

# ─── Configurações ───────────────────────────────────────────
INICIO_MARCO = datetime(2026, 3, 1)

# Caminho do arquivo parquet (USAR ESTE COMO FONTE)
CAMINHO_PARQUET = Path(__file__).parent.parent / "dados" / "EMPILHADO_MATRICULAS.parquet"

COLUNAS_ALVO = [
    "id_aluno", "nm_aluno", "data_nascimento",
    "inep_escola", "nm_escola", "nm_regional", "nm_municipio",
    "nome_turma", "nome_ano_escolaridade",
    "id_ano", "id_ano_letivo", "num_ano_letivo",
    "dt_matricula", "dt_enturmacao",
    "situacao_enturmacao", "situacao_matricula", "tipo_atendimento",
    "cpf", "data_referencia",    # ✅ Para extrair a semana
]

# Ordem EXATA de colunas do Excel modelo (aba Alertas_Data_Quality)
COLUNAS_EXCEL = [
    "Regional", "Escola", "Nome_Estudante", "Data_Nascimento",      
    "Matricula_Retroativa", "Detalhe_Retroativa",
    "IDs", "Status_ID", "Detalhe_Mudanca_ID",
    "Status_Matricula", "Detalhe_Mudanca_Matricula",
    "Status_Frequencia", "Detalhe_Frequencia",
    "Primeira_Aparicao", "Ultima_Aparicao", "Total_Semanas",
]

# =============================================================================
# FUNÇÕES DE APOIO
# =============================================================================
def _converter_para_data(valor):
    """Converte string em datetime (ISO ou BR). Retorna None em caso de erro."""
    if pd.isna(valor) or str(valor).strip() == "":
        return None
    valor = str(valor).strip()
    try:
        if "-" in valor:
            return datetime.strptime(valor[:10], "%Y-%m-%d")
        if "/" in valor:
            return datetime.strptime(valor[:10], "%d/%m/%Y")
    except Exception:
        return None
    return None


def _normalizar_data_str(d) -> str:
    """Converte datetime para string DD/MM/YYYY."""
    obj = _converter_para_data(d)
    return obj.strftime("%d/%m/%Y") if obj else "DATA_INVALIDA"


def _extrair_semana(data_atualizacao):
    """Extrai a data de atualização como DD/MM/YYYY (representa a semana)."""
    if pd.isna(data_atualizacao):
        return None
    try:
        dt = _converter_para_data(data_atualizacao)
        return dt.strftime("%d/%m/%Y") if dt else None
    except:
        return None


# =============================================================================
# LEITURA DO PARQUET
# =============================================================================
@st.cache_data(show_spinner=False)
def _carregar_parquet() -> pd.DataFrame:
    """
    Lê EMPILHADO_MATRICULAS.parquet, aplica filtros de negócio.
    Retorna DataFrame com todas as semanas para auditoria temporal.
    """
    try:
        if not os.path.exists(CAMINHO_PARQUET):
            st.error(f"❌ Arquivo não encontrado: {CAMINHO_PARQUET}")
            return pd.DataFrame()

        st.info(f"📁 Carregando: {os.path.basename(CAMINHO_PARQUET)}")
        
        # Ler colunas disponíveis
        cols_disponiveis = pd.read_parquet(CAMINHO_PARQUET).columns.tolist()
        cols_ler = [c for c in COLUNAS_ALVO if c in cols_disponiveis]
        
        df = pd.read_parquet(CAMINHO_PARQUET, columns=cols_ler)
        df.columns = df.columns.str.strip().str.lower()
        
        st.success(f"✅ Carregadas {len(df):,} linhas")
        st.info(f"📊 Colunas disponíveis: {len(cols_ler)}/{len(COLUNAS_ALVO)}")
        
        # FILTRO 1: Ano letivo 2026
        if "num_ano_letivo" in df.columns:
            num_clean = df["num_ano_letivo"].astype(str).str.replace(r"\D", "", regex=True)
            df = df[num_clean.str.contains("2026", na=False)]
            st.info(f"📅 Após filtro ano 2026: {len(df):,} linhas")

        # FILTRO 2: Em curso (matrícula)
        if "situacao_matricula" in df.columns:
            sit = df["situacao_matricula"].astype(str).str.lower().str.replace(r"\s+", "", regex=True)
            df = df[sit.str.contains("emcurso", na=False)]
            st.info(f"📋 Após filtro matrícula em curso: {len(df):,} linhas")
        
        # FILTRO 3: Em curso (enturmação)
        if "situacao_enturmacao" in df.columns:
            sit_ent = df["situacao_enturmacao"].astype(str).str.lower().str.replace(r"\s+", "", regex=True)
            df = df[sit_ent.str.contains("emcurso", na=False)]
            st.info(f"📍 Após filtro enturmação em curso: {len(df):,} linhas")

        # FILTRO 4: Regular
        if "tipo_atendimento" in df.columns:
            atend = df["tipo_atendimento"].astype(str).str.lower().str.replace(r"\s+", "", regex=True)
            df = df[atend.str.contains("regular", na=False)]
            st.info(f"🎓 Após filtro atendimento regular: {len(df):,} linhas")

        return df.reset_index(drop=True)

    except Exception as e:
        st.error(f"❌ Erro ao carregar parquet: {e}")
        return pd.DataFrame()


# =============================================================================
# CONSTRUÇÃO DO HISTÓRICO POR PESSOA (COM SEMANAS)
# =============================================================================
def _construir_historico(df: pd.DataFrame) -> tuple[dict, list[str], dict]:
    """
    Agrupa dados por (nome, data_nascimento, semana).
    Retorna (historico_pessoas, datas_processadas, escolas_identificadas).
    
    historico_pessoas = {
        (nome, nasc): {
            'semanas': {
                '01/03/2026': {'datas_mat': set, 'datas_ent': set, 'ids': set, 'deficiencias': set},
                '08/03/2026': {...}
            },
            'escola': str,
            'regional': str
        }
    }
    """
    if df.empty:
        st.warning("⚠️ DataFrame vazio. Verifique os filtros.")
        return {}, [], {}

    historico_pessoas = {}
    datas_processadas = []
    escolas_identificadas = {}

    st.info(f"🔄 Processando {len(df):,} registros...")
    barra = st.progress(0, text="Construindo histórico...")

    for idx, row in df.iterrows():
        if idx % 10000 == 0:
            barra.progress(min(idx / len(df), 1.0), text=f"Processados {idx:,}/{len(df):,}")

        # Extrair semana da data_atualizacao
        dt_fmt = _extrair_semana(row.get("data_referencia"))
        if not dt_fmt:
            continue

        # Normalizar nome
        nome = " ".join(str(row.get("nm_aluno", "")).upper().split())
        nasc = _normalizar_data_str(row.get("data_nascimento"))
        chave = (nome, nasc)

        if not nome or nome == "":
            continue

        # Inicializar pessoa
        if chave not in historico_pessoas:
            historico_pessoas[chave] = {
                "semanas": {},
                "escola": "",
                "regional": "",
            }

        # Atualizar escola/regional
        historico_pessoas[chave]["escola"] = (
            str(row.get("nm_escola", "")).strip().upper() if "nm_escola" in df.columns else ""
        )
        historico_pessoas[chave]["regional"] = (
            str(row.get("nm_regional", "")).strip().upper() if "nm_regional" in df.columns else ""
        )

        # Inicializar semana
        if dt_fmt not in historico_pessoas[chave]["semanas"]:
            historico_pessoas[chave]["semanas"][dt_fmt] = {
                "datas_mat": set(),
                "datas_ent": set(),
                "ids": set(),
                "deficiencias": set(),  # ✅ NOVO
            }

        # Adicionar ID
        historico_pessoas[chave]["semanas"][dt_fmt]["ids"].add(
            str(row.get("id_aluno", "")).replace(".0", "").strip()
        )

        # Adicionar datas
        dt_mat_obj = _converter_para_data(row.get("dt_matricula"))
        if dt_mat_obj:
            historico_pessoas[chave]["semanas"][dt_fmt]["datas_mat"].add(dt_mat_obj)

        dt_ent_obj = _converter_para_data(row.get("dt_enturmacao"))
        if dt_ent_obj:
            historico_pessoas[chave]["semanas"][dt_fmt]["datas_ent"].add(dt_ent_obj)

            # Rastrear datas processadas
        if dt_fmt not in datas_processadas:
            datas_processadas.append(dt_fmt)

        # Rastrear escolas
        inep = str(row.get("inep_escola", "")).strip()
        if inep and inep.lower() != "nan" and inep not in escolas_identificadas:
            escolas_identificadas[inep] = {
                "Data_Aparecimento": dt_fmt,
                "INEP": inep,
                "Escola": str(row.get("nm_escola", "")).strip(),
                "Regional": str(row.get("nm_regional", "")).strip(),
            }

    barra.empty()

    # Ordenar datas
    datas_processadas.sort(key=lambda x: datetime.strptime(x, "%d/%m/%Y"))

    st.success(
        f"✅ Histórico construído: {len(historico_pessoas):,} pessoas "
        f"em {len(datas_processadas)} semana(s)"
    )

    return historico_pessoas, datas_processadas, escolas_identificadas


# =============================================================================
# AUDITORIA — APLICAÇÃO DAS 6 REGRAS
# =============================================================================
def _auditar_wide(historico_pessoas: dict, datas_processadas: list[str]) -> pd.DataFrame:
    """
    Aplica as 6 regras de auditoria temporal.
    Retorna DF wide (1 linha por aluno com alertas).
    """
    linhas = []
    total = len(historico_pessoas)
    barra = st.progress(0, text="Auditando...")

    for i, ((nome, nasc), info) in enumerate(historico_pessoas.items(), 1):
        if i % 1000 == 0:
            barra.progress(min(i / total, 1.0), text=f"Auditados {i:,}/{total:,}")

        semanas_visto = sorted(
            list(info["semanas"].keys()),
            key=lambda x: datetime.strptime(x, "%d/%m/%Y"),
        )
        if not semanas_visto:
            continue

        # ── Regra 1: Matrícula Retroativa ──
        primeira_dt = datetime.strptime(semanas_visto[0], "%d/%m/%Y")
        alerta_tardia, detalhe_tardia = "Não", ""
        if primeira_dt >= INICIO_MARCO:
            mats_fev = [
                d for d in info["semanas"][semanas_visto[0]]["datas_mat"]
                if d.year == 2026 and d.month == 2 and d.day <= 15
            ]
            if mats_fev:
                alerta_tardia = "Sim"
                detalhe_tardia = (
                    f"Apareceu em {semanas_visto[0]} com matrícula retroativa de "
                    f"{mats_fev[0].strftime('%d/%m/%Y')}"
                )

        # ── Regra 2: Frequência Io-iô ──
        indices_tempo = sorted([datas_processadas.index(s) for s in semanas_visto])
        teve_buraco, detalhe_ioio = False, ""
        gaps = []
        for j in range(len(indices_tempo) - 1):
            if indices_tempo[j + 1] > indices_tempo[j] + 1:
                teve_buraco = True
                faltantes = datas_processadas[indices_tempo[j] + 1 : indices_tempo[j + 1]]
                gaps.append(
                    f"Ausente entre {semanas_visto[j]} e {semanas_visto[j+1]} "
                    f"(Faltou em: {', '.join(faltantes)})"
                )
        if gaps:
            detalhe_ioio = " | ".join(gaps)

        # ── Regra 3: Mudança de ID ──
        todos_ids = set()
        for s in semanas_visto:
            todos_ids.update(info["semanas"][s]["ids"])

        mudancas_id_log = []
        for j in range(1, len(semanas_visto)):
            ids_atu = info["semanas"][semanas_visto[j]]["ids"]
            ids_ant = info["semanas"][semanas_visto[j - 1]]["ids"]
            if ids_atu != ids_ant:
                mudancas_id_log.append(
                    f"Em {semanas_visto[j]} mudou de {ids_ant} para {ids_atu}"
                )

        status_id = "Alterado" if len(todos_ids) > 1 else "Único"
        detalhe_id = " | ".join(mudancas_id_log) if mudancas_id_log else ""

        # ── Regra 4: Alteração em dt_matricula ──
        status_mat, detalhe_mat = "Ok", ""
        for j in range(1, len(semanas_visto)):
            d_atu = info["semanas"][semanas_visto[j]]["datas_mat"]
            d_ant = info["semanas"][semanas_visto[j - 1]]["datas_mat"]
            if d_atu and d_ant and d_atu != d_ant:
                min_atu = min(d_atu)
                min_ant = min(d_ant)
                if min_atu < min_ant:
                    status_mat = "ALERTA: Retrocedeu"
                elif min_atu > min_ant and status_mat == "Ok":
                    status_mat = "Alterada (Avançou)"
                detalhe_mat = (
                    f"Em {semanas_visto[j]} passou de "
                    f"{min_ant.strftime('%d/%m/%Y')} para {min_atu.strftime('%d/%m/%Y')}"
                )

        # ✅ Critério de inclusão COMPLETO
        tem_alerta = (
            teve_buraco
            or len(todos_ids) > 1
            or status_mat != "Ok"
            or alerta_tardia == "Sim"         
        )
        if not tem_alerta:
            continue

        linhas.append({
            "Regional": info["regional"] if info["regional"] else "",
            "Escola": info["escola"] if info["escola"] else "",
            "Nome_Estudante": nome,
            "Data_Nascimento": nasc,
            "Matricula_Retroativa": alerta_tardia,
            "Detalhe_Retroativa": detalhe_tardia,
            "IDs": ", ".join(sorted(todos_ids)),
            "Status_ID": status_id,
            "Detalhe_Mudanca_ID": detalhe_id,
            "Status_Matricula": status_mat,
            "Detalhe_Mudanca_Matricula": detalhe_mat,
            "Status_Frequencia": "Io-iô" if teve_buraco else "Regular",
            "Detalhe_Frequencia": detalhe_ioio,
            "Primeira_Aparicao": semanas_visto[0],
            "Ultima_Aparicao": semanas_visto[-1],
            "Total_Semanas": len(semanas_visto),
        })

    barra.empty()

    if not linhas:
        return pd.DataFrame(columns=COLUNAS_EXCEL)

    return pd.DataFrame(linhas)[COLUNAS_EXCEL]


# =============================================================================
# CONVERSÃO WIDE → LONG
# =============================================================================
def _wide_para_long(df_wide: pd.DataFrame) -> pd.DataFrame:
    """
    Converte DF wide em formato longo (1 linha por alerta).
    """
    if df_wide.empty:
        return pd.DataFrame()

    linhas = []
    for _, r in df_wide.iterrows():
        base = {
            "nm_regional": r["Regional"],
            "nm_escola": r["Escola"],
            "nm_aluno": r["Nome_Estudante"],
            "id_aluno": r["IDs"],
            "data_nascimento": r["Data_Nascimento"],
        }
        if r["Status_Matricula"] != "Ok":
            linhas.append({
                **base,
                "alerta": ALERTA_DT_MATRICULA,
                "detalhe": f"{r['Status_Matricula']} — {r['Detalhe_Mudanca_Matricula']}",
            })
        if r["Matricula_Retroativa"] == "Sim":
            linhas.append({
                **base,
                "alerta": ALERTA_RETROATIVA,
                "detalhe": r["Detalhe_Retroativa"],
            })
        
        if r["Status_ID"] == "Alterado":
            linhas.append({
                **base,
                "alerta": ALERTA_MUDANCA_ID,
                "detalhe": r["Detalhe_Mudanca_ID"],
            })
        if r["Status_Frequencia"] == "Io-iô":
            linhas.append({
                **base,
                "alerta": ALERTA_IOIO,
                "detalhe": r["Detalhe_Frequencia"],
            })
        

    return pd.DataFrame(linhas)


# =============================================================================
# GERAÇÃO DO EXCEL EM MEMÓRIA
# =============================================================================
def _gerar_excel_bytes(df_alertas: pd.DataFrame, df_escolas: pd.DataFrame) -> bytes:
    """Gera o XLSX em memória."""
    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
        if df_alertas.empty:
            pd.DataFrame({"Aviso": ["Nenhuma inconsistência encontrada."]}) \
                .to_excel(writer, sheet_name="Alertas_Data_Quality", index=False)
        else:
            df_alertas.to_excel(writer, sheet_name="Alertas_Data_Quality", index=False)

        if not df_escolas.empty:
            df_escolas.to_excel(writer, sheet_name="Escolas_2026", index=False)

    buffer.seek(0)
    return buffer.getvalue()


# =============================================================================
# FUNÇÃO PÚBLICA — FUNÇÃO PRINCIPAL CHAMADA POR app.py
# =============================================================================
def checar_alteracao_dt_matricula() -> pd.DataFrame:
    """
    UI Streamlit integrada:
      1. Carrega EMPILHADO_MATRICULAS.parquet
      2. Aplica filtros de negócio (2026, Em curso, Regular)
      3. Constrói histórico temporal (por semana)
      4. Aplica 6 regras de auditoria
      5. Exibe resultados + permite download Excel
      6. Retorna DF longo para resumo agregado do app.py
    
    Retorna:
      DataFrame com colunas: nm_regional, nm_escola, nm_aluno, id_aluno, 
                            data_nascimento, alerta, detalhe
    """
    st.markdown("---")
    st.markdown("#### 📅 Auditoria Temporal — Análise de Inconsistências")
    st.markdown(
        "Analisando histórico temporal de matrículas com **6 regras de validação**"
    )

    # Carregar dados
    with st.spinner("📊 Carregando dados..."):
        df = _carregar_parquet()

    if df.empty:
        st.error("❌ Não foi possível carregar os dados.")
        return pd.DataFrame()

    # Construir histórico
    with st.spinner("🔄 Construindo histórico temporal..."):
        historico, datas_processadas, escolas = _construir_historico(df)

    if not historico:
        st.warning("⚠️ Nenhum aluno foi processado.")
        return pd.DataFrame()

    # Executar auditoria
    with st.spinner("🔍 Aplicando regras de auditoria..."):
        df_wide = _auditar_wide(historico, datas_processadas)

    df_escolas = pd.DataFrame(list(escolas.values())) if escolas else pd.DataFrame()

    # Apresentar resultados
    if df_wide.empty:
        st.success(
            f"✅ **Nenhuma inconsistência** encontrada em {len(historico):,} aluno(s) "
            f"em {len(datas_processadas)} semana(s)."
        )
        excel_bytes = _gerar_excel_bytes(df_wide, df_escolas)
        st.download_button(
            label="📥 Baixar Excel (sem alertas)",
            data=excel_bytes,
            file_name="Auditoria_Temporal_Evolucao.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            key="dl_auditoria_vazia",
        )
        return pd.DataFrame()

    # Mostrar resumo
    st.success(
        f"⚠️ **{len(df_wide):,} aluno(s) com inconsistência** "
        f"(de {len(historico):,} analisados em {len(datas_processadas)} semana(s))"
    )

    # Download
    excel_bytes = _gerar_excel_bytes(df_wide, df_escolas)
    st.download_button(
        label="📥 Baixar Excel completo",
        data=excel_bytes,
        file_name="Auditoria_Temporal_Evolucao.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        key="dl_auditoria",
    )

    # Prévia
    with st.expander("👁 Prévia (primeiros 50 registros)", expanded=False):
        st.dataframe(df_wide.head(50), use_container_width=True, hide_index=True)

    # Retornar formato LONG
    df_long = _wide_para_long(df_wide).reset_index(drop=True)
    st.info(f"📌 {len(df_long):,} alerta(s) individuais extraído(s)")
    
    return df_long
