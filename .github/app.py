import streamlit as st
import pandas as pd
import os
import plotly.graph_objects as go
from pathlib import Path
from PIL import Image
from datetime import datetime

###########################################################################
from carregador import COLUNAS_OBRIGATORIAS

# ─── Caminhos ──────────────────────────────────────────────
BASE_DIR = Path(__file__).parent / "dados_graficos"

PARQUET_RESULTADOS = BASE_DIR / "resultados.parquet"
PARQUET_RESUMO     = BASE_DIR / "resumo.parquet"

# ─── Rótulos fixos de todos os alertas possíveis ───────────
TODOS_ALERTAS = [
    "CPF inválido/em branco",    
    "Matrícula duplicada",
    "Deficiência sem descrição",
    "Descrição de deficiência indevida",
    "dt_matricula alterada",
    "Matrícula retroativa",    
    "Mudança de id_aluno",
    "Frequência io-iô",
    "Sem_autodeclaracao_racial"    
]

# ─── Nomes amigáveis para exibição na UI ──────────────────
NOMES_AMIGAVEIS = {
    "CPF inválido/em branco":          "Alunos sem CPF",
    "Matrícula duplicada":             "Matrículas Duplicadas",
    "Deficiência sem descrição":       "Flag Deficiência sem Descrição",
    "Descrição de deficiência indevida": "Descrição de Deficiência Inconsistente",
    "dt_matricula alterada":           "Datas de Matrícula Alterada",
    "Matrícula retroativa":            "Alunos com Data de Matrícula Retroativa",
    "Mudança de id_aluno":             "Alunos com ID Alterado",
    "Frequência io-iô":                "Alunos com ausência e retorno registrados na base SEGES",
    "Sem_autodeclaracao_racial":       "Alunos sem Autodeclaração Racial",
}
# Inverso: nome amigável → chave interna (para filtrar os dados)
_NOMES_INVERSO = {v: k for k, v in NOMES_AMIGAVEIS.items()}

# ─── Colunas exibidas no detalhamento por tipo de alerta ───
COLUNAS_POR_ALERTA = {
    "Deficiência sem descrição": [
        "nm_regional", "inep_escola", "nm_escola",
        "id_aluno", "nm_aluno", "fl_deficiencia", "dc_deficiencia", "alerta",
    ],
    "Descrição de deficiência indevida": [
        "nm_regional", "inep_escola", "nm_escola",
        "id_aluno", "nm_aluno", "fl_deficiencia", "dc_deficiencia", "alerta",
    ],
    "Sem_autodeclaracao_racial": [
        "nm_regional", "nm_municipio", "inep_escola",
        "nm_escola", "nome_turma", "id_aluno", "nm_aluno", "dc_cor_raca", "alerta",
    ],
    "dt_matricula alterada": [
        "nm_regional", "nm_municipio", "inep_escola", "nm_escola",
        "nome_turma", "id_aluno", "nm_aluno", "data_nascimento",
        "primeira_aparicao", "ultima_aparicao", "total_semanas",
        "alerta", "detalhe",
    ],
    "Matrícula retroativa": [
        "nm_regional", "nm_municipio", "inep_escola", "nm_escola",
        "nome_turma", "id_aluno", "nm_aluno", "data_nascimento",
        "primeira_aparicao", "alerta", "detalhe",
    ],    
    "Mudança de id_aluno": [
        "nm_regional", "nm_municipio", "inep_escola", "nm_escola",
        "nome_turma", "id_aluno", "nm_aluno", "data_nascimento",
        "total_semanas", "alerta", "detalhe",
    ],
    "Frequência io-iô": [
        "nm_regional", "nm_municipio", "inep_escola", "nm_escola",
        "nome_turma", "id_aluno", "nm_aluno", "data_nascimento",
        "primeira_aparicao", "ultima_aparicao", "total_semanas",
        "alerta", "detalhe",
    ]
}

COLUNAS_PADRAO = [
    "nm_regional", "nm_municipio", "inep_escola",
    "nm_escola", "id_aluno", "nm_aluno",
    "nome_turma", "cpf", "alerta",
]

# ─── Config da página ───────────────────────────────────────
st.set_page_config(
    page_title="Painel de Alertas SEDU",
    layout="wide",
    initial_sidebar_state="collapsed",
)
#---------- Ajuste do titulo e imagen -----------
import base64

def img_to_base64(path):
    with open(path, "rb") as f:
        return base64.b64encode(f.read()).decode()

logo_b64 = img_to_base64(Path(__file__).parent / "logo.png")

st.markdown(f"""
    <div style="display:flex; align-items:center; gap:16px; margin-bottom:1rem;">
        <img src="data:image/png;base64,{logo_b64}" width="120">
        <h1 style="margin:0;">Painel de Alertas — SEDU</h1>
    </div>
""", unsafe_allow_html=True)
#-----------------------------------------------
# ─── Leitura dos parquets ────────────────
def _carregar_parquets():
    resultados = pd.read_parquet(PARQUET_RESULTADOS)
    resumo     = pd.read_parquet(PARQUET_RESUMO)
    return resultados, resumo

# ─── Verifica existência dos arquivos ───────────────────────
if not PARQUET_RESULTADOS.exists() or not PARQUET_RESUMO.exists():
    st.error("⚠️ Arquivos de resultado não encontrados.")
    st.info("Execute `python processar.py` para gerar os dados antes de abrir o painel.")
    st.stop()

# ─── Timestamp da última atualização ────────────────────────
ts = PARQUET_RESULTADOS.stat().st_mtime
ultima_atualizacao = datetime.fromtimestamp(ts).strftime("%d/%m/%Y %H:%M")
st.caption(f"🕒 Última atualização: {ultima_atualizacao}")

# ─── Carrega dados ───────────────────────────────────────────
with st.spinner("Carregando dados..."):
    resultados, resumo = _carregar_parquets()

st.success(f"{len(resultados):,} alertas carregados.")

# ─── Resumo ──────────────────────────────────────────────────
st.subheader("📊 Resumo")
resumo_exibir = resumo.copy()
resumo_exibir["alerta"] = resumo_exibir["alerta"].map(NOMES_AMIGAVEIS).fillna(resumo_exibir["alerta"])
st.dataframe(resumo_exibir, use_container_width=True, hide_index=True)

# ─── Gráfico de barras empilhadas ───────────────────────
try:
    caminho_excel_grafico = BASE_DIR / "dados_graficos.xlsx"
    
    df_raw = pd.read_excel(caminho_excel_grafico, header=0)
    total = pd.to_numeric(df_raw.iloc[0, 1], errors="coerce")  # pega 181975 de B1

    df_graf = df_raw.iloc[:, 2:4].copy()  # colunas C e D
    df_graf.columns = ["alerta", "erros"]
    df_graf = df_graf.dropna(subset=["alerta"])
    df_graf["alerta"] = df_graf["alerta"].map(NOMES_AMIGAVEIS).fillna(df_graf["alerta"])
    df_graf["erros"] = pd.to_numeric(df_graf["erros"], errors="coerce").fillna(0).astype(int)
    df_graf["total"] = total
    df_graf["pct_erros"] = (df_graf["erros"] / df_graf["total"] * 100).fillna(0).round(1)
    df_graf["pct_resto"] = 100 - df_graf["pct_erros"]

    fig = go.Figure()
    fig.add_bar(
        x=df_graf["alerta"],
        y=df_graf["pct_erros"],
        name="Erros",
        marker_color="#378ADD",
        customdata=df_graf[["total", "erros", "pct_erros"]].values,
        hovertemplate="<b>%{x}</b><br>Erros: %{customdata[1]:,}<br>Total: %{customdata[0]:,}<br>%: %{customdata[2]:.1f}%<extra></extra>",
    )
    fig.add_bar(
        x=df_graf["alerta"],
        y=df_graf["pct_resto"],
        name="Total (base)",
        marker_color="#E8E6E0",
        customdata=df_graf[["total", "erros", "pct_erros"]].values,
        hovertemplate="<b>%{x}</b><br>Erros: %{customdata[1]:,}<br>Total: %{customdata[0]:,}<br>%: %{customdata[2]:.1f}%<extra></extra>",
    )
    fig.update_layout(
        barmode="stack",
        height=450,
        margin=dict(l=10, r=10, t=10, b=120),
        xaxis=dict(tickangle=-35, title=""),
        yaxis=dict(tickformat=".0f", ticksuffix="%", title=""),
        legend=dict(orientation="h", y=1.05, x=0),
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
    )
    st.plotly_chart(fig, use_container_width=True)
except Exception as e:
    st.warning(f"Gráfico indisponível: {e}")
    # ────────────────────────────────────────────────────────
# ─── Detalhamento ────────────────────────────────────────────
if resultados.empty:
    st.success("✅ Nenhuma inconsistência encontrada!")
else:
    st.subheader("🔍 Detalhamento")
    # Monta opções com nome amigável, mas guarda a chave interna para filtrar
    alertas_unicos = sorted(resultados["alerta"].unique())
    opcoes_amigaveis = ["Todos"] + [NOMES_AMIGAVEIS.get(a, a) for a in alertas_unicos]
    filtro_amigavel = st.selectbox("Filtrar por tipo de alerta:", opcoes_amigaveis)

    # Converte nome amigável de volta para chave interna
    filtro = _NOMES_INVERSO.get(filtro_amigavel, filtro_amigavel)

    df_filtrado = (
        resultados if filtro_amigavel == "Todos"
        else resultados[resultados["alerta"] == filtro]
    )

    cols_alerta = COLUNAS_POR_ALERTA.get(filtro, COLUNAS_PADRAO)
    cols_exibir = [c for c in cols_alerta if c in df_filtrado.columns]
    if not cols_exibir:
        cols_exibir = list(df_filtrado.columns)

    st.dataframe(df_filtrado[cols_exibir], use_container_width=True)

    csv_erros = df_filtrado[cols_exibir].to_csv(index=False).encode("utf-8-sig")
    nome_arquivo = f"alertas_{filtro_amigavel.replace(' ', '_').replace('/', '_')}.csv"
    st.download_button("⬇️ Baixar erros em CSV", csv_erros, nome_arquivo, "text/csv")
