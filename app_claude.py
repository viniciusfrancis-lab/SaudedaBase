import streamlit as st
import streamlit.components.v1 as components
import pandas as pd
import plotly.graph_objects as go
from pathlib import Path

# ── Configuração da página ────────────────────────────────────────────────────
st.set_page_config(
    page_title="Painel de Alertas — SEDU",
    page_icon="https://cdn.es.gov.br/images/logo/governo/brasao/right/Brasao_Governo_100.png",
    layout="wide",
)

# ── Paleta SEDU ───────────────────────────────────────────────────────────────
TEAL_DARK  = "#1a6b7a"
TEAL       = "#1e8fa5"
TEAL_MID   = "#2aabc4"
TEAL_LIGHT = "#c8eaf0"
TEAL_PALE  = "#eaf6f9"
ROSE       = "#c97080"
ROSE_LIGHT = "#f5dde2"
GREEN      = "#1D9E75"
GREEN_DARK = "#0F6E56"
MUTED      = "#718096"
TEXT       = "#2d3748"

# Cores de prioridade (vivas, para badges e realces)
PRIO_HIGH      = "#e05a6d"
PRIO_HIGH_BG   = "#fdeef0"
PRIO_MED       = "#1e8fa5"
PRIO_MED_BG    = "#e6f4f8"
PRIO_LOW       = "#1D9E75"
PRIO_LOW_BG    = "#e3f5ee"

# Ícone (emoji) por rótulo de alerta — identificação visual rápida
ALERTA_ICON = {
    "Datas de Matrícula Alterada":              "📅",
    "Ausência e retorno (freq. io-iô)":         "🔁",
    "Alunos Desenturmado":                      "👤",
    "Matrícula Retroativa":                     "⏪",
    "Alunos com ID Alterado":                   "🆔",
    "Alunos sem CPF":                           "🪪",
    "Sem Autodeclaração Racial":                "📋",
    "Descrição de Deficiência Inconsistente":   "♿",
    "Flag Deficiência sem Descrição":           "🚩",
    "Matrículas Duplicadas":                    "📑",
}

# ── CSS global ────────────────────────────────────────────────────────────────
st.markdown(f"""
<style>
  /* Layout base */
  .block-container {{ padding-top: 1.25rem !important; padding-left: 3rem !important; padding-right: 3rem !important; max-width: 100% !important; }}
  section[data-testid="stSidebar"] {{ display: none; }}
  header[data-testid="stHeader"] {{ display: none; }}
  footer {{ display: none; }}

  /* Header e banner são cards centralizados (NÃO full-bleed).
     Respeitam o padding do .block-container e têm cantos arredondados.
     Mantemos o gap vertical natural entre eles. */

  /* Header institucional */
  .sedu-header {{
    background: {TEAL_DARK};
    padding: 14px 22px;
    display: flex;
    align-items: center;
    justify-content: space-between;
    min-height: 60px;
    border-radius: 10px;
  }}
  .sedu-header-left {{
    display: flex;
    align-items: center;
    gap: 14px;
  }}
  .sedu-header-logo img {{
    height: 38px;
    width: auto;
  }}
  .sedu-header-divider {{
    width: 1px;
    height: 32px;
    background: rgba(255,255,255,0.25);
  }}
  .sedu-header-title {{
    font-size: 18px;
    font-weight: 600;
    color: #fff;
    margin: 0;
  }}
  .sedu-header-sub {{
    font-size: 11px;
    color: rgba(255,255,255,0.6);
    margin: 2px 0 0 0;
  }}
  .sedu-badge {{
    font-size: 11px;
    color: rgba(255,255,255,0.75);
    background: rgba(255,255,255,0.12);
    border-radius: 20px;
    padding: 4px 12px;
    display: flex;
    align-items: center;
    gap: 6px;
    white-space: nowrap;
  }}
  .sedu-dot {{
    width: 7px; height: 7px;
    border-radius: 50%;
    background: #5DCAA5;
    display: inline-block;
  }}

  /* Banner total de alertas */
  .sedu-banner {{
    background: linear-gradient(135deg, {TEAL} 0%, {TEAL_MID} 100%);
    padding: 16px 22px;
    display: flex;
    align-items: center;
    gap: 14px;
    border-radius: 10px;
  }}
  .sedu-banner .sedu-banner-num {{
    font-size: 26px !important;
    font-weight: 600 !important;
    color: #fff !important;
    margin: 0 !important;
    line-height: 1.1 !important;
  }}
  .sedu-banner .sedu-banner-label {{
    font-size: 11px !important;
    color: rgba(255,255,255,0.8) !important;
    margin: 2px 0 0 0 !important;
  }}
  .sedu-banner-chips {{
    margin-left: auto;
    display: flex;
    gap: 10px;
    align-items: center;
  }}
  .sedu-chip {{
    display: flex;
    flex-direction: column;
    align-items: center;
    background: rgba(255,255,255,0.14);
    border-radius: 10px;
    padding: 7px 16px;
    min-width: 78px;
  }}
  .sedu-chip-num {{
    font-size: 17px;
    font-weight: 700;
    color: #fff;
    line-height: 1.1;
  }}
  .sedu-chip-label {{
    font-size: 9px;
    letter-spacing: 0.05em;
    text-transform: uppercase;
    color: rgba(255,255,255,0.75);
    margin-top: 2px;
  }}
  .sedu-chip .sedu-chip-dot {{
    display: inline-block;
    width: 7px; height: 7px;
    border-radius: 50%;
    margin-right: 4px;
    vertical-align: middle;
  }}

  /* Corpo */
  .sedu-body {{
    padding: 1.25rem 0 2rem;
  }}
  .sedu-section-title {{
    font-size: 10px;
    font-weight: 600;
    letter-spacing: 0.07em;
    color: {TEAL_DARK};
    text-transform: uppercase;
    margin: 1.25rem 0 0.75rem 0;
    padding-bottom: 6px;
    border-bottom: 2px solid {TEAL_LIGHT};
  }}

  /* Cards de métrica */
  .sedu-cards-grid {{
    display: grid;
    grid-template-columns: repeat(auto-fill, minmax(170px, 1fr));
    gap: 12px;
    margin-top: 0.25rem;
  }}
  .sedu-card {{
    position: relative;
    background: #fff;
    border: 0.5px solid #d4e9ee;
    border-radius: 10px;
    border-top: 3px solid {TEAL};
    padding: 12px 14px 14px;
    transition: transform .16s ease, box-shadow .16s ease, border-color .16s ease;
    cursor: default;
    overflow: hidden;
  }}
  .sedu-card::after {{
    content: "";
    position: absolute;
    right: -28px; bottom: -28px;
    width: 78px; height: 78px;
    border-radius: 50%;
    background: {TEAL_PALE};
    opacity: .55;
  }}
  .sedu-card:hover {{
    transform: translateY(-3px);
    box-shadow: 0 8px 22px rgba(26,107,122,0.16);
    border-color: {TEAL_MID};
  }}
  .sedu-card.warn {{ border-top-color: {PRIO_HIGH}; }}
  .sedu-card.warn::after {{ background: {PRIO_HIGH_BG}; }}
  .sedu-card.ok   {{ border-top-color: {PRIO_LOW}; }}
  .sedu-card.ok::after {{ background: {PRIO_LOW_BG}; }}

  .sedu-card-top {{
    display: flex;
    align-items: flex-start;
    justify-content: space-between;
    gap: 8px;
    position: relative;
    z-index: 1;
  }}
  .sedu-card-icon {{
    font-size: 16px;
    line-height: 1;
    flex-shrink: 0;
  }}
  .sedu-card-badge {{
    font-size: 9px;
    font-weight: 700;
    letter-spacing: 0.04em;
    text-transform: uppercase;
    padding: 2px 7px;
    border-radius: 20px;
    white-space: nowrap;
  }}
  .sedu-card-badge.high {{ background: {PRIO_HIGH_BG}; color: {PRIO_HIGH}; }}
  .sedu-card-badge.med  {{ background: {PRIO_MED_BG};  color: {PRIO_MED}; }}
  .sedu-card-badge.low  {{ background: {PRIO_LOW_BG};  color: {PRIO_LOW}; }}
  .sedu-card-label {{
    position: relative;
    z-index: 1;
    font-size: 11px;
    color: {MUTED};
    margin: 8px 0 4px 0;
    line-height: 1.3;
    display: -webkit-box;
    -webkit-line-clamp: 2;
    -webkit-box-orient: vertical;
    overflow: hidden;
    min-height: 2.6em;
  }}
  .sedu-card-value {{
    position: relative;
    z-index: 1;
    font-size: 26px;
    font-weight: 700;
    color: {TEAL_DARK};
    margin: 0;
    line-height: 1.1;
  }}
  .sedu-card-value.warn {{ color: {PRIO_HIGH}; }}
  .sedu-card-value.ok   {{ color: {GREEN_DARK}; }}

  /* Tabela */
  .sedu-table-wrap {{
    border: 0.5px solid #d4e9ee;
    border-radius: 10px;
    overflow: hidden;
    margin-top: 0.5rem;
  }}
  .sedu-table {{
    width: 100%;
    border-collapse: collapse;
    font-size: 13px;
  }}
  .sedu-table thead th {{
    font-size: 10px;
    font-weight: 600;
    color: #fff;
    background: {TEAL_DARK};
    padding: 9px 14px;
    text-align: left;
  }}
  .sedu-table thead th.right {{ text-align: right; }}
  .sedu-table tbody tr {{ border-bottom: 0.5px solid #e8f4f7; }}
  .sedu-table tbody tr:last-child {{ border-bottom: none; }}
  .sedu-table tbody tr:hover {{ background: {TEAL_PALE}; }}
  .sedu-table tbody td {{ padding: 9px 14px; color: {TEXT}; }}
  .sedu-table tbody td.right {{ text-align: right; font-weight: 600; }}
  .sedu-table tbody td.high  {{ color: {ROSE}; }}
  .sedu-table tbody td.med   {{ color: {TEAL}; }}
  .sedu-table tbody td.ok    {{ color: {GREEN_DARK}; }}
  .pill {{
    font-size: 10px;
    padding: 2px 8px;
    border-radius: 20px;
    font-weight: 600;
    margin-left: 6px;
    vertical-align: middle;
  }}
  .pill.high {{ background: {ROSE_LIGHT}; color: {ROSE}; }}
  .pill.med  {{ background: {TEAL_LIGHT}; color: {TEAL_DARK}; }}
  .pill.low  {{ background: #E1F5EE; color: {GREEN_DARK}; }}
</style>
""", unsafe_allow_html=True)


# ── Dados ─────────────────────────────────────────────────────────────────────
RESUMO_PATH          = Path(__file__).parent / "dados_graficos" / "resumo.parquet"
XLSX_ULTIMA_APARICAO = Path(__file__).parent / "dados_graficos" / "alerta_ultima_aparicao.xlsx"

# Mapeamento: nome exato da coluna "alerta" no parquet -> (rótulo exibição, prioridade)
ALERTA_META = {
    "CPF inválido/em branco":            ("Alunos sem CPF",                          "média"),
    "Matrícula duplicada":               ("Matrículas Duplicadas",                    "baixa"),
    "Deficiência sem descrição":         ("Flag Deficiência sem Descrição",           "baixa"),
    "Descrição de deficiência indevida": ("Descrição de Deficiência Inconsistente",   "baixa"),
    "dt_matricula alterada":             ("Datas de Matrícula Alterada",              "alta"),
    "Matrícula retroativa":              ("Matrícula Retroativa",                     "alta"),
    "Mudança de id_aluno":               ("Alunos com ID Alterado",                   "média"),
    "Frequência io-iô":                  ("Ausência e retorno (freq. io-iô)",         "alta"),
    "Sem_autodeclaracao_racial":         ("Sem Autodeclaração Racial",                "média"),
    "Última aparição anterior a hoje":   ("Alunos Desenturmado",                      "média"),
}

@st.cache_data
def carregar_dados():
    df_raw = pd.read_parquet(RESUMO_PATH)

    # Normaliza colunas
    df_raw.columns = df_raw.columns.str.strip()
    if "Qtd" in df_raw.columns:
        df_raw = df_raw.rename(columns={"Qtd": "qtd"})

    # Aplica rótulos de exibição e prioridade via mapeamento
    chave = df_raw["alerta"].str.strip()
    df_raw["alerta"]     = chave.map(lambda x: ALERTA_META.get(x, (x, "baixa"))[0])
    df_raw["prioridade"] = chave.map(lambda x: ALERTA_META.get(x, (x, "baixa"))[1])
    df_raw["qtd"]        = pd.to_numeric(df_raw["qtd"], errors="coerce").fillna(0).astype(int)

    # Ordena por quantidade decrescente
    df_raw = df_raw.sort_values("qtd", ascending=False).reset_index(drop=True)

    total_qtd = df_raw["qtd"].sum()
    df_raw["pct"] = (df_raw["qtd"] / total_qtd * 100).round(1) if total_qtd > 0 else 0.0

    return df_raw[["alerta", "qtd", "pct", "prioridade"]]

df = carregar_dados()
total = int(df["qtd"].sum())

# Última atualização
from datetime import datetime
ultima_atualizacao = datetime.now().strftime("%d/%m/%Y %H:%M")


# ── Wrapper principal ───────────────────────────────────────────────────────
st.markdown('<div class="sedu-page">', unsafe_allow_html=True)

# ── Header ────────────────────────────────────────────────────────────────────
ultima_data = datetime.now().strftime("%d/%m/%Y")
ultima_hora = datetime.now().strftime("%H:%M")

st.markdown(f"""
<div class="sedu-header">
  <div class="sedu-header-left">
    <div class="sedu-header-logo">
      <img src="https://cdn.es.gov.br/images/logo/governo/brasao/right/Brasao_Governo_240.png"
           alt="Brasão do Governo do Espírito Santo" />
    </div>
    <div class="sedu-header-divider"></div>
    <div>
      <p class="sedu-header-title">Painel de Alertas — SEDU</p>
      <p class="sedu-header-sub">Secretaria da Educação · ES</p>
    </div>
  </div>
  <div class="sedu-badge">
    <span class="sedu-dot"></span>
    {ultima_data} · {ultima_hora}
  </div>
</div>
""", unsafe_allow_html=True)


# ── Banner ────────────────────────────────────────────────────────────────────
# Quebra por prioridade para os chips de status
_qtd_por_prio = df.groupby("prioridade")["qtd"].sum()
qtd_alta  = int(_qtd_por_prio.get("alta", 0))
qtd_media = int(_qtd_por_prio.get("média", 0))
qtd_baixa = int(_qtd_por_prio.get("baixa", 0))
fmt = lambda v: f"{v:,}".replace(",", ".")

st.markdown(f"""
<div class="sedu-banner">
  <div style="font-size:22px; color:rgba(255,255,255,0.85);">⚠</div>
  <div>
    <div class="sedu-banner-num">{fmt(total)}</div>
    <div class="sedu-banner-label">alertas carregados nesta atualização</div>
  </div>
  <div class="sedu-banner-chips">
    <div class="sedu-chip">
      <span class="sedu-chip-num"><span class="sedu-chip-dot" style="background:{PRIO_HIGH};"></span>{fmt(qtd_alta)}</span>
      <span class="sedu-chip-label">Alta</span>
    </div>
    <div class="sedu-chip">
      <span class="sedu-chip-num"><span class="sedu-chip-dot" style="background:#bdeaf2;"></span>{fmt(qtd_media)}</span>
      <span class="sedu-chip-label">Média</span>
    </div>
    <div class="sedu-chip">
      <span class="sedu-chip-num"><span class="sedu-chip-dot" style="background:#8fe3c4;"></span>{fmt(qtd_baixa)}</span>
      <span class="sedu-chip-label">Baixa</span>
    </div>
  </div>
</div>
""", unsafe_allow_html=True)


# ── Corpo ─────────────────────────────────────────────────────────────────────
st.markdown('<div class="sedu-body">', unsafe_allow_html=True)

# Seção: cards de visão geral — gerado dinamicamente a partir do parquet
def _card_cls(prioridade):
    return "warn" if prioridade == "alta" else ("ok" if prioridade == "baixa" and False else "")

# Mapeia prioridade para classe CSS do card, do valor e do badge
CARD_CLS  = {"alta": "warn", "média": "", "baixa": "ok"}
VAL_CLS   = {"alta": "warn", "média": "",  "baixa": "ok"}
BADGE_CLS = {"alta": "high", "média": "med", "baixa": "low"}
BADGE_TXT = {"alta": "Alta", "média": "Média", "baixa": "Baixa"}

st.markdown('<p class="sedu-section-title">visão geral</p>', unsafe_allow_html=True)

# Grid responsivo: cards se ajustam à largura disponível e quebram para a
# próxima linha quando não couberem mais (auto-fill + minmax no CSS).
cards_html = ['<div class="sedu-cards-grid">']
for row in df.itertuples():
    ccls = CARD_CLS[row.prioridade]
    vcls = VAL_CLS[row.prioridade]
    bcls = BADGE_CLS[row.prioridade]
    btxt = BADGE_TXT[row.prioridade]
    icon = ALERTA_ICON.get(row.alerta, "•")
    valor_fmt = f"{row.qtd:,}".replace(",", ".")
    cards_html.append(
        f'<div class="sedu-card {ccls}">'
        f'<div class="sedu-card-top">'
        f'<span class="sedu-card-icon">{icon}</span>'
        f'<span class="sedu-card-badge {bcls}">{btxt}</span>'
        f'</div>'
        f'<p class="sedu-card-label" title="{row.alerta}">{row.alerta}</p>'
        f'<p class="sedu-card-value {vcls}">{valor_fmt}</p>'
        f'</div>'
    )
cards_html.append('</div>')
st.markdown("".join(cards_html), unsafe_allow_html=True)


# Seção: gráfico
st.markdown('<p class="sedu-section-title" style="margin-top:1.5rem;">total de alertas por categoria</p>', unsafe_allow_html=True)

# Lê total de alunos da célula B2 do xlsx
try:
    _df_xlsx = pd.read_excel(Path(__file__).parent / "dados_graficos" / "dados_graficos.xlsx", header=None)
    total_alunos = int(_df_xlsx.iloc[1, 1])
except Exception:
    total_alunos = int(df["qtd"].max() * 1.15)

bar_colors = [
    PRIO_HIGH if p == "alta" else (PRIO_MED if p == "média" else PRIO_LOW)
    for p in df["prioridade"]
]

fig = go.Figure(go.Bar(
    x=df["qtd"],
    y=df["alerta"],
    orientation="h",
    marker=dict(
        color=bar_colors,
        line=dict(width=0),
        cornerradius=4,
    ),
    text=[f"{v:,}".replace(",", ".") for v in df["qtd"]],
    textposition="outside",
    textfont=dict(size=12, color=TEXT),
    customdata=df["pct"],
    hovertemplate="<b>%{y}</b><br>%{x:,} alertas · %{customdata:.1f}% do total<extra></extra>",
))

fig.update_layout(
    height=280,
    margin=dict(l=0, r=80, t=10, b=10),
    paper_bgcolor="white",
    plot_bgcolor="white",
    font=dict(family="sans-serif", size=12, color=MUTED),
    xaxis=dict(
        range=[0, total_alunos],
        gridcolor="#f0f4f5",
        showline=False,
        zeroline=False,
        tickfont=dict(color=TEXT, size=12),
        tickvals=[0, 20000, 40000, 60000, 80000, 100000, 120000, 140000, 160000, total_alunos],
        ticktext=["0", "20.000", "40.000", "60.000", "80.000", "100.000", "120.000", "140.000", "160.000", f"{total_alunos:,}".replace(",", ".")],
    ),
    yaxis=dict(
        autorange="reversed",
        showgrid=False,
        showline=False,
        tickfont=dict(color=TEXT, size=13),
    ),
    showlegend=False,
)

# Legenda manual
total_fmt = f"{total_alunos:,}".replace(",", ".")
legend_html = f"""
<div style="display:flex; align-items:center; justify-content:space-between;">
  <div style="display:flex; gap:18px; font-size:12px; color:{MUTED};">
    <span><span style="display:inline-block; width:10px; height:10px; border-radius:2px; background:{PRIO_HIGH}; margin-right:5px; vertical-align:middle;"></span>Alta prioridade</span>
    <span><span style="display:inline-block; width:10px; height:10px; border-radius:2px; background:{PRIO_MED}; margin-right:5px; vertical-align:middle;"></span>Média prioridade</span>
    <span><span style="display:inline-block; width:10px; height:10px; border-radius:2px; background:{PRIO_LOW}; margin-right:5px; vertical-align:middle;"></span>Baixa prioridade</span>
  </div>
  <div style="display:flex; flex-direction:column; align-items:flex-end; border-left:2px solid {TEAL_LIGHT}; padding-left:16px;">
    <span style="font-size:10px; font-weight:600; letter-spacing:0.06em; text-transform:uppercase; color:{MUTED};">Total de alunos</span>
    <span style="font-size:16px; font-weight:700; color:{TEAL_DARK}; line-height:1.2;">{total_fmt}</span>
  </div>
</div>
"""
st.markdown(f'<div style="background:#fff; border:0.5px solid #d4e9ee; border-radius:10px; padding:14px 18px;">{legend_html}</div>', unsafe_allow_html=True)
st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})



######################### OPEN GRAFICO TEMPORAL ##############################################
st.markdown('<p class="sedu-section-title" style="margin-top:1.5rem; margin-bottom:-0.5rem;">quantidade de alunos por tipo de saída</p>', unsafe_allow_html=True)
# LER O EXCEL
df = pd.read_excel('grafico_temporal/base_temporal_alunos.xlsx')

# AGRUPAR E PIVOTAR
df_agrupado = df.groupby(['data_referencia', 'tipo']).size().reset_index(name='quantidade')
df_pivot = df_agrupado.pivot(index='data_referencia', columns='tipo', values='quantidade').fillna(0)

# GARANTIR ORDEM DE COLUNAS
colunas_desejadas = ['Sumiu', 'Abandono', 'Transferência']
df_pivot = df_pivot[[col for col in colunas_desejadas if col in df_pivot.columns]]

# CRIAR O GRÁFICO
fig = go.Figure()

cores = {
    'Sumiu': '#636EFA',           # Azul
    'Abandono': '#EF553B',        # Vermelho
    'Transferência': '#00CC96'    # Verde
}

for tipo in df_pivot.columns:
    fig.add_trace(go.Scatter(
        x=df_pivot.index,
        y=df_pivot[tipo],
        mode='lines+markers',
        name=tipo,
        line=dict(color=cores.get(tipo, '#000'), width=2),
        marker=dict(size=6)
    ))

# LAYOUT
fig.update_layout(
    xaxis_title='Mês/Ano',
    yaxis_title='Quantidade de Alunos',
    hovermode='x unified',
    template='plotly',
    height=600,
    plot_bgcolor='white',
    paper_bgcolor='white',
    font=dict(color='#000000'),
    xaxis=dict(
        showgrid=True, 
        gridwidth=1, 
        gridcolor='#E5E5E5',
        title_font=dict(color='#000000'),
        tickfont=dict(color='#000000')
    ),
    yaxis=dict(
        showgrid=True, 
        gridwidth=1, 
        gridcolor='#E5E5E5',
        title_font=dict(color='#000000'),
        tickfont=dict(color='#000000')
    ),
    legend=dict(
        x=0.98,
        y=0.98,
        bgcolor='rgba(255,255,255,0.9)',
        bordercolor='black',
        borderwidth=1,
        font=dict(color='#000000')
    ),
    margin=dict(l=60, r=60, t=80, b=60)
)

# Adicionar borda
fig.add_shape(
    type='rect',
    xref='paper',
    yref='paper',
    x0=0,
    y0=0,
    x1=1,
    y1=1,
    line=dict(color='#E5E5E5', width=2),
    fillcolor='rgba(0,0,0,0)',
    layer='below'
)

# EXIBIR NO STREAMLIT
st.markdown("""
    <style>
        [data-testid="stPlotlyChart"] div {
            border-radius: 15px;
            overflow: hidden;
            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
        }
    </style>
    """, unsafe_allow_html=True)


st.plotly_chart(fig, use_container_width=True)


########################### CLOSE GRAFICO TEMPORAL############################################

##########  OPEN GRAFICO SANKEY  ###########################
with open('sankey/sankey_visual_v2.html', 'r', encoding='utf-8') as f:
    html_content = f.read()

# Envolve o conteúdo em uma div com estilo
html_estilizado = f"""
<div style="
    border-radius: 20px;
    overflow: hidden;
    box-shadow: 0 4px 12px rgba(0,0,0,0.1);
    background: white;
    padding: 10px;
">
    {html_content}
</div>
"""

components.html(html_estilizado, height=850, scrolling=False)
##########  FECHAR GRAFICO SANKEY  ###########################

# ── Seção: detalhamento por aluno (resultados.parquet) ───────────────────────
st.markdown('<p class="sedu-section-title" style="margin-top:1.5rem;">detalhamento</p>', unsafe_allow_html=True)

PARQUET_RESULTADOS = Path(__file__).parent / "dados_graficos" / "resultados.parquet"

COLUNAS_EXIBIR = [
    "alerta", "detalhe",
    "nm_regional", "nm_municipio", "nm_escola",
    "nome_ano_escolaridade", "dc_turno",
    "nm_aluno", "ra_aluno", "cpf",
    "dt_matricula", "situacao_matricula",
]

@st.cache_data
def carregar_resultados():
    df_r = pd.read_parquet(PARQUET_RESULTADOS)
    df_r.columns = df_r.columns.str.strip()
    cols = [c for c in COLUNAS_EXIBIR if c in df_r.columns]
    return df_r[cols].copy()

df_res = carregar_resultados()

# ── Filtros encadeados ────────────────────────────────────────────────────────
col_f1, col_f2, col_f3, col_dl = st.columns([2, 2, 3, 1])

alertas_disponiveis = sorted(df_res["alerta"].dropna().unique().tolist())
with col_f1:
    filtro_alerta = st.selectbox("Tipo de alerta", ["Todos"] + alertas_disponiveis)

regionais_disponiveis = sorted(df_res["nm_regional"].dropna().unique().tolist()) if "nm_regional" in df_res.columns else []
with col_f2:
    filtro_regional = st.selectbox("Regional", ["Todas"] + regionais_disponiveis)

# aplica filtros progressivamente para popular o selectbox de escola
df_filtrado = df_res.copy()
if filtro_alerta != "Todos":
    df_filtrado = df_filtrado[df_filtrado["alerta"] == filtro_alerta]
if filtro_regional != "Todas":
    df_filtrado = df_filtrado[df_filtrado["nm_regional"] == filtro_regional]

escolas_disponiveis = sorted(df_filtrado["nm_escola"].dropna().unique().tolist()) if "nm_escola" in df_filtrado.columns else []
with col_f3:
    filtro_escola = st.selectbox("Escola", ["Todas"] + escolas_disponiveis)
if filtro_escola != "Todas":
    df_filtrado = df_filtrado[df_filtrado["nm_escola"] == filtro_escola]

# ── Botão CSV alinhado aos filtros ────────────────────────────────────────────
csv = df_filtrado.to_csv(index=False, sep=";", encoding="utf-8-sig")
with col_dl:
    st.markdown("<div style='margin-top:1.6rem;'>", unsafe_allow_html=True)
    st.download_button(
        label="⬇ CSV",
        data=csv,
        file_name="alertas_detalhamento.csv",
        mime="text/csv",
        use_container_width=True,
    )
    st.markdown("</div>", unsafe_allow_html=True)

# ── Contagem ──────────────────────────────────────────────────────────────────
n = len(df_filtrado)
st.markdown(
    f'<p style="font-size:12px; color:{MUTED}; margin:4px 0 10px;">' +
    f'{n:,} registro(s) encontrado(s)'.replace(",", ".") + "</p>",
    unsafe_allow_html=True,
)

# ── Tabela HTML estilizada (igual ao mockup) ──────────────────────────────────
LABELS_COLUNAS = {
    "alerta":                "Alerta",
    "detalhe":               "Detalhe",
    "nm_regional":           "Regional",
    "nm_municipio":          "Município",
    "nm_escola":             "Escola",
    "nome_ano_escolaridade": "Ano/Série",
    "dc_turno":              "Turno",
    "nm_aluno":              "Aluno",
    "ra_aluno":              "RA",
    "cpf":                   "CPF",
    "dt_matricula":          "Dt. Matrícula",
    "situacao_matricula":    "Situação",
}

PILL_ALERTA = {
    "CPF inválido/em branco":            ("média", "med"),
    "Matrícula duplicada":               ("baixa", "low"),
    "Deficiência sem descrição":         ("baixa", "low"),
    "Descrição de deficiência indevida": ("baixa", "low"),
    "dt_matricula alterada":             ("alta",  "high"),
    "Matrícula retroativa":              ("alta",  "high"),
    "Mudança de id_aluno":               ("média", "med"),
    "Frequência io-iô":                  ("alta",  "high"),
    "Sem_autodeclaracao_racial":         ("média", "med"),
}

cols_presentes = [c for c in COLUNAS_EXIBIR if c in df_filtrado.columns and df_filtrado[c].notna().any()] # ===> aqui em detalhamento somente aparece colunas com dados preenchido, ignora os vazios
df_exibir = df_filtrado[cols_presentes].rename(columns=LABELS_COLUNAS).reset_index(drop=True)

# Cabeçalho — sticky no scroll vertical
thead = "".join(
    f'<th style="font-size:11px;font-weight:600;color:#fff;background:{TEAL_DARK};'
    f'padding:9px 14px;text-align:left;white-space:nowrap;position:sticky;top:0;z-index:2;">{c}</th>'
    for c in df_exibir.columns
)

# Linhas — máx 500 para não travar o browser
tbody = ""
df_head = df_filtrado.head(500).reset_index(drop=True)
df_exibir_head = df_exibir.head(500).reset_index(drop=True)

for i, row in df_exibir_head.iterrows():
    cells = ""
    for col_name, val in row.items():
        val_str = "" if pd.isna(val) else str(val)
        td_base = f'padding:9px 14px;white-space:nowrap;vertical-align:middle;border-bottom:0.5px solid #e8f4f7;color:#2d3748;background:#ffffff;'
        if col_name == "Alerta":
            alerta_orig = df_head.at[i, "alerta"] if "alerta" in df_head.columns else ""
            ptxt, pcls = PILL_ALERTA.get(alerta_orig, ("baixa", "low"))
            cells += (
                f'<td style="{td_base}">' 
                f'<span style="white-space:nowrap;">{val_str} <span class="pill {pcls}">{ptxt}</span></span></td>'
            )
        else:
            cells += f'<td style="{td_base}">{val_str}</td>'
    tbody += f'<tr>{cells}</tr>'

tabela_html = f"""
<div style="border:0.5px solid #d4e9ee;border-radius:10px;overflow:auto;max-height:500px;background:#ffffff;">
  <table style="border-collapse:collapse;font-size:13px;min-width:100%;">
    <thead><tr>{thead}</tr></thead>
    <tbody>{tbody}</tbody>
  </table>
</div>
"""
st.markdown(tabela_html, unsafe_allow_html=True)

# aviso se truncado
if len(df_filtrado) > 500:
    st.caption(f"Exibindo 500 de {len(df_filtrado):,} registros. Use o CSV para ver todos.".replace(",", "."))



st.markdown('</div>', unsafe_allow_html=True)  # fecha .sedu-body
st.markdown('</div>', unsafe_allow_html=True)  # fecha .sedu-page
