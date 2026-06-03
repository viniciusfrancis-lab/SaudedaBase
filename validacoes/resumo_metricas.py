# validacoes/resumo_metricas.py
"""
Gerador de tabela de resumo de métricas do Painel de Alertas.

Calcula e exibe em uma tabela Streamlit:
  - Total de CPFs únicos
  - Total de Alunos únicos
  - Total de Matrículas
  - Total de Deficiência sem descrição
  - Total de Descrição de deficiência indevida
  - Total de Cronologia inválida
  - Total de ID Aluno alterados
  - Total de Frequência io-iô
  - Total de Sem autodeclaração racial

Uso:
    from validacoes.resumo_metricas import (
        gerar_resumo_metricas,
        exibir_tabela_metricas,
        exportar_metricas_excel,
        consolidar_alertas
    )
"""

import pandas as pd
import streamlit as st
from datetime import datetime


def gerar_resumo_metricas(df_base: pd.DataFrame, alertas_por_tipo: dict) -> pd.DataFrame:
    """
    Gera tabela de resumo com métricas consolidadas.
    
    Args:
        df_base: DataFrame bruto carregado (base SEGES do dia)
        alertas_por_tipo: Dicionário {tipo_alerta: DataFrame com erros}
                         Exemplo: {'CPF inválido/em branco': df_cpf_erros, ...}
    
    Returns:
        DataFrame com 2 colunas: ['Métrica', 'Quantidade']
    """
    
    metricas = {}
    
    # ─── Contadores da base completa ───
    metricas['Total de CPFs'] = df_base['cpf'].nunique() if 'cpf' in df_base.columns else 0
    metricas['Total de Alunos'] = df_base['nm_aluno'].nunique() if 'nm_aluno' in df_base.columns else 0
    metricas['Total de Matrículas'] = len(df_base)
    
    # ─── Contadores de alertas (a partir do dict alertas_por_tipo) ───
    metricas['CPF inválido/em branco'] = (
        len(alertas_por_tipo.get('CPF inválido/em branco', pd.DataFrame()))
    )
    metricas['Aluno sem turma'] = (
        len(alertas_por_tipo.get('Aluno sem turma', pd.DataFrame()))
    )
    metricas['Matrícula duplicada'] = (
        len(alertas_por_tipo.get('Matrícula duplicada', pd.DataFrame()))
    )
    metricas['Deficiência sem descrição'] = (
        len(alertas_por_tipo.get('Deficiência sem descrição', pd.DataFrame()))
    )
    metricas['Descrição de deficiência indevida'] = (
        len(alertas_por_tipo.get('Descrição de deficiência indevida', pd.DataFrame()))
    )
    metricas['dt_matricula alterada'] = (
        len(alertas_por_tipo.get('dt_matricula alterada', pd.DataFrame()))
    )
    metricas['Matrícula retroativa'] = (
        len(alertas_por_tipo.get('Matrícula retroativa', pd.DataFrame()))
    )
    metricas['Cronologia inválida'] = (
        len(alertas_por_tipo.get('Cronologia inválida', pd.DataFrame()))
    )
    metricas['Mudança de id_aluno'] = (
        len(alertas_por_tipo.get('Mudança de id_aluno', pd.DataFrame()))
    )
    metricas['Frequência io-iô'] = (
        len(alertas_por_tipo.get('Frequência io-iô', pd.DataFrame()))
    )
    metricas['Sem_autodeclaracao_racial'] = (
        len(alertas_por_tipo.get('Sem_autodeclaracao_racial', pd.DataFrame()))
    )
    metricas['Deficiência cruzada'] = (
        len(alertas_por_tipo.get('Deficiência cruzada', pd.DataFrame()))
    )
    
    # Converter para DataFrame
    df_resumo = pd.DataFrame([
        {'Métrica': k, 'Quantidade': v}
        for k, v in metricas.items()
    ])
    
    return df_resumo


def exibir_tabela_metricas(df_resumo: pd.DataFrame, timestamp: str = None):
    """
    Exibe a tabela de resumo no Streamlit com formatação amigável.
    
    Args:
        df_resumo: DataFrame gerado por gerar_resumo_metricas()
        timestamp: Timestamp da execução (ex: "07/05/2026 14:35")
    """
    
    st.markdown("---")
    st.markdown("### 📊 Resumo de Métricas — Data Quality")
    
    if timestamp:
        st.caption(f"📅 Atualizado em: {timestamp}")
    
    # Dividir em 2 colunas para layout melhor
    col1, col2 = st.columns(2)
    
    # ─── COLUNA 1: Métricas da Base ───
    with col1:
        st.markdown("#### 📈 Base de Dados")
        metricas_base = df_resumo[df_resumo['Métrica'].isin([
            'Total de CPFs',
            'Total de Alunos',
            'Total de Matrículas'
        ])]
        
        for _, row in metricas_base.iterrows():
            st.metric(label=row['Métrica'], value=f"{row['Quantidade']:,}")
    
    # ─── COLUNA 2: Top Alertas ───
    with col2:
        st.markdown("#### ⚠️ Alertas (Top 3)")
        
        # Filtrar apenas alertas (exclui as 3 métricas de base)
        df_alertas = df_resumo[~df_resumo['Métrica'].isin([
            'Total de CPFs',
            'Total de Alunos',
            'Total de Matrículas'
        ])]
        
        # Ordenar por quantidade decrescente e pegar top 3
        top3 = df_alertas.nlargest(3, 'Quantidade')
        
        for _, row in top3.iterrows():
            st.metric(label=row['Métrica'], value=f"{row['Quantidade']:,}")
    
    # ─── TABELA COMPLETA ───
    st.markdown("#### 📋 Todas as Métricas")
    
    # Formatação: adicionar separador de milhares
    df_display = df_resumo.copy()
    df_display['Quantidade'] = df_display['Quantidade'].apply(lambda x: f"{x:,}")
    
    st.dataframe(
        df_display,
        use_container_width=True,
        hide_index=True,
        column_config={
            'Métrica': st.column_config.TextColumn(label='Métrica', width=350),
            'Quantidade': st.column_config.TextColumn(label='Quantidade', width=150),
        }
    )
    
    return df_resumo


def exportar_metricas_excel(df_resumo: pd.DataFrame, nome_arquivo: str = None) -> bytes:
    """
    Exporta as métricas para um arquivo Excel em memória.
    
    Args:
        df_resumo: DataFrame de métricas
        nome_arquivo: Nome do arquivo (sem .xlsx)
    
    Returns:
        Bytes do arquivo Excel
    """
    import io
    
    if nome_arquivo is None:
        timestamp = datetime.now().strftime("%d_%m_%Y_%H%M%S")
        nome_arquivo = f"Resumo_Metricas_{timestamp}"
    
    buffer = io.BytesIO()
    
    with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
        df_resumo.to_excel(writer, sheet_name='Métricas', index=False)
        
        # Obter worksheet para aplicar formatação
        worksheet = writer.sheets['Métricas']
        
        # Ajustar largura das colunas
        worksheet.column_dimensions['A'].width = 40
        worksheet.column_dimensions['B'].width = 15
        
        # Negrito no header
        from openpyxl.styles import Font, PatternFill, Alignment
        
        header_fill = PatternFill(start_color="366092", end_color="366092", fill_type="solid")
        header_font = Font(bold=True, color="FFFFFF")
        center_align = Alignment(horizontal="center", vertical="center")
        
        for cell in worksheet[1]:
            cell.fill = header_fill
            cell.font = header_font
            cell.alignment = center_align
    
    buffer.seek(0)
    return buffer.getvalue()


# ─── Função auxiliar: agregador de alertas ───
def consolidar_alertas(*dfs_alertas) -> dict:
    """
    Consolida múltiplos DataFrames de alertas em um dicionário por tipo.
    
    Uso:
        from validacoes.cpf import checar_cpf
        from validacoes.turma import checar_sem_turma
        from validacoes.flag_deficiencia import checar_flag_deficiencia
        from validacoes.cor_raca import checar_sem_autodeclaracao_racial
        from validacoes.matricula import checar_duplicatas
        
        df_cpf = checar_cpf(df)
        df_turma = checar_sem_turma(df)
        df_deficiencia = checar_flag_deficiencia(df)
        df_cor = checar_sem_autodeclaracao_racial(df)
        df_matricula = checar_duplicatas(df)
        
        alertas = consolidar_alertas(
            ('CPF inválido/em branco', df_cpf),
            ('Aluno sem turma', df_turma),
            ('Deficiência sem descrição', df_deficiencia),
            ...
        )
    
    Returns:
        Dicionário: {tipo_alerta: DataFrame}
    """
    resultado = {}
    
    for tipo_alerta, df in dfs_alertas:
        if df is not None and not df.empty:
            resultado[tipo_alerta] = df
        else:
            resultado[tipo_alerta] = pd.DataFrame()
    
    return resultado
