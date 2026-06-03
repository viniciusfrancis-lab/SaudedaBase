"""
limpar_duplicatas_ultima_aparicao.py
════════════════════════════════════════════════════════════════════════════════

OBJETIVO:
  Remove do resultados.parquet os alertas de "Última aparição anterior a hoje"
  para alunos que JÁ aparecem com estes alertas de auditoria:
    - Frequência io-iô
    - dt_matricula alterada
    - Matrícula retroativa
    - Mudança de id_aluno

LÓGICA:
  1. Carrega resultados.parquet
  2. Identifica alunos que têm os 4 alertas de auditoria acima
  3. Remove linhas onde:
     - alerta = "Última aparição anterior a hoje"
     - E nm_aluno está na lista de alunos com os 4 alertas
  4. Salva o arquivo limpo

USO:
  python limpar_duplicatas_ultima_aparicao.py

ARQUIVOS:
  Entrada:  dados_graficos/resultados.parquet
  Backup:   dados_graficos/resultados_backup_YYYYMMDD_HHMMSS.parquet
  Saída:    dados_graficos/resultados.parquet (sobrescreve)
"""

from pathlib import Path
from datetime import datetime
import pandas as pd
import shutil

# ── Configurações ──────────────────────────────────────────────────────────────
BASE_DIR = Path(__file__).parent / "dados_graficos"
ARQUIVO_RESULTADOS = BASE_DIR / "resultados.parquet"

# Alertas de auditoria que EXCLUEM de "Última aparição"
ALERTAS_AUDITORIA = [
    "Frequência io-iô",
    "dt_matricula alterada",
    "Matrícula retroativa",
    "Mudança de id_aluno",
]

ALERTA_ULTIMA_APARICAO = "Última aparição anterior a hoje"


def fazer_backup(arquivo: Path) -> Path:
    """Cria backup com timestamp antes de modificar."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_path = arquivo.with_name(f"{arquivo.stem}_backup_{timestamp}{arquivo.suffix}")
    shutil.copy2(arquivo, backup_path)
    return backup_path


def limpar_duplicatas(arquivo: Path, dry_run: bool = False) -> pd.DataFrame:
    """
    Remove alertas de "Última aparição" para alunos que têm alertas de auditoria.
    
    Args:
        arquivo: Caminho para resultados.parquet
        dry_run: Se True, não salva o arquivo, apenas mostra o que seria feito
    
    Returns:
        DataFrame limpo
    """
    print("=" * 80)
    print("LIMPEZA DE DUPLICATAS - ÚLTIMA APARIÇÃO")
    print("=" * 80)
    
    # 1. Verifica se arquivo existe
    if not arquivo.exists():
        print(f"\n❌ Arquivo não encontrado: {arquivo}")
        return pd.DataFrame()
    
    # 2. Carrega resultados
    print(f"\n📂 Carregando: {arquivo}")
    df = pd.read_parquet(arquivo)
    print(f"   {len(df):,} registros carregados")
    
    if df.empty:
        print("\n⚠️  Arquivo vazio, nada a fazer.")
        return df
    
    # Verifica colunas necessárias
    if "alerta" not in df.columns:
        print("\n❌ Coluna 'alerta' não encontrada no arquivo!")
        return df
    
    if "nm_aluno" not in df.columns:
        print("\n❌ Coluna 'nm_aluno' não encontrada no arquivo!")
        return df
    
    # 3. Estatísticas iniciais
    print("\n" + "─" * 80)
    print("📊 ESTATÍSTICAS INICIAIS")
    print("─" * 80)
    
    total_inicial = len(df)
    alunos_unicos_inicial = df["nm_aluno"].nunique()
    
    print(f"\n  Total de registros: {total_inicial:,}")
    print(f"  Alunos únicos: {alunos_unicos_inicial:,}")
    print(f"  Tipos de alerta: {df['alerta'].nunique()}")
    
    print("\n  Breakdown por tipo de alerta:")
    contagem_inicial = df.groupby("alerta").size().sort_values(ascending=False)
    for alerta, qtd in contagem_inicial.items():
        print(f"    • {alerta:<50} {qtd:>8,}")
    
    # 4. Identifica alunos com alertas de auditoria
    print("\n" + "─" * 80)
    print("🔍 IDENTIFICANDO ALUNOS COM ALERTAS DE AUDITORIA")
    print("─" * 80)
    
    # Filtra apenas os alertas de auditoria
    df_auditoria = df[df["alerta"].isin(ALERTAS_AUDITORIA)]
    
    if df_auditoria.empty:
        print("\n  ℹ️  Nenhum alerta de auditoria encontrado.")
        print("     Nada a filtrar.")
        return df
    
    # Lista de alunos únicos que têm alertas de auditoria
    alunos_com_auditoria = set(df_auditoria["nm_aluno"].dropna().unique())
    
    print(f"\n  Alertas de auditoria encontrados:")
    for alerta in ALERTAS_AUDITORIA:
        qtd = len(df_auditoria[df_auditoria["alerta"] == alerta])
        if qtd > 0:
            print(f"    • {alerta:<50} {qtd:>8,}")
    
    print(f"\n  Total de alunos com alertas de auditoria: {len(alunos_com_auditoria):,}")
    
    # 5. Identifica registros de "Última aparição" a serem removidos
    print("\n" + "─" * 80)
    print("🗑️  IDENTIFICANDO REGISTROS PARA REMOÇÃO")
    print("─" * 80)
    
    # Conta quantos alertas de "Última aparição" existem
    mask_ultima_aparicao = df["alerta"] == ALERTA_ULTIMA_APARICAO
    qtd_ultima_aparicao_total = mask_ultima_aparicao.sum()
    
    print(f"\n  Alertas de '{ALERTA_ULTIMA_APARICAO}': {qtd_ultima_aparicao_total:,}")
    
    if qtd_ultima_aparicao_total == 0:
        print("  ℹ️  Nenhum alerta de 'Última aparição' encontrado.")
        print("     Nada a fazer.")
        return df
    
    # Identifica quais devem ser removidos (tem alerta de auditoria)
    mask_remover = (
        (df["alerta"] == ALERTA_ULTIMA_APARICAO) &
        (df["nm_aluno"].isin(alunos_com_auditoria))
    )
    
    qtd_remover = mask_remover.sum()
    qtd_manter = qtd_ultima_aparicao_total - qtd_remover
    
    print(f"  Serão REMOVIDOS: {qtd_remover:,} (alunos que têm alertas de auditoria)")
    print(f"  Serão MANTIDOS:  {qtd_manter:,} (alunos sem alertas de auditoria)")
    
    if qtd_remover == 0:
        print("\n  ℹ️  Nenhum registro para remover.")
        print("     Todos os alertas de 'Última aparição' são de alunos sem alertas de auditoria.")
        return df
    
    # Mostra exemplos de alunos que serão afetados
    if qtd_remover > 0:
        print("\n  📋 Exemplos de alunos que serão removidos de 'Última aparição':")
        df_remover = df[mask_remover]
        exemplos = df_remover["nm_aluno"].unique()[:5]
        for i, nome in enumerate(exemplos, 1):
            # Mostra quais alertas de auditoria o aluno tem
            alertas_aluno = df_auditoria[df_auditoria["nm_aluno"] == nome]["alerta"].unique()
            print(f"    {i}. {nome}")
            print(f"       → Tem alertas: {', '.join(alertas_aluno)}")
    
    # 6. Remove duplicatas
    print("\n" + "─" * 80)
    print("✂️  REMOVENDO DUPLICATAS")
    print("─" * 80)
    
    df_limpo = df[~mask_remover].copy()
    
    print(f"\n  Registros removidos: {qtd_remover:,}")
    print(f"  Registros restantes: {len(df_limpo):,}")
    
    # 7. Estatísticas finais
    print("\n" + "─" * 80)
    print("📊 ESTATÍSTICAS FINAIS")
    print("─" * 80)
    
    total_final = len(df_limpo)
    alunos_unicos_final = df_limpo["nm_aluno"].nunique()
    
    print(f"\n  Total de registros: {total_final:,} (antes: {total_inicial:,})")
    print(f"  Alunos únicos: {alunos_unicos_final:,} (antes: {alunos_unicos_inicial:,})")
    print(f"  Redução: {total_inicial - total_final:,} registros ({((total_inicial - total_final) / total_inicial * 100):.1f}%)")
    
    print("\n  Breakdown FINAL por tipo de alerta:")
    contagem_final = df_limpo.groupby("alerta").size().sort_values(ascending=False)
    for alerta, qtd in contagem_final.items():
        qtd_antes = contagem_inicial.get(alerta, 0)
        diferenca = qtd - qtd_antes
        if diferenca != 0:
            print(f"    • {alerta:<50} {qtd:>8,} ({diferenca:+,})")
        else:
            print(f"    • {alerta:<50} {qtd:>8,}")
    
    # 8. Salva ou mostra dry-run
    print("\n" + "─" * 80)
    if dry_run:
        print("🔍 DRY-RUN MODE - NENHUMA ALTERAÇÃO FEITA")
        print("─" * 80)
        print("\n  ℹ️  Este foi apenas um teste.")
        print("     Para aplicar as mudanças, execute sem --dry-run")
    else:
        print("💾 SALVANDO ARQUIVO")
        print("─" * 80)
        
        # Backup
        print(f"\n  Criando backup...")
        backup_path = fazer_backup(arquivo)
        print(f"  ✅ Backup salvo: {backup_path.name}")
        
        # Salva arquivo limpo
        print(f"\n  Salvando arquivo limpo...")
        df_limpo.to_parquet(arquivo, index=False)
        print(f"  ✅ Arquivo atualizado: {arquivo}")
    
    print("\n" + "=" * 80)
    print("✅ LIMPEZA CONCLUÍDA")
    print("=" * 80)
    
    return df_limpo


def main():
    """Execução principal com opção de dry-run."""
    import sys
    
    # Verifica se é dry-run
    dry_run = "--dry-run" in sys.argv or "-d" in sys.argv
    
    if dry_run:
        print("\n⚠️  MODO DRY-RUN ATIVADO - Nenhuma alteração será feita\n")
    
    df_limpo = limpar_duplicatas(ARQUIVO_RESULTADOS, dry_run=dry_run)
    
    if not df_limpo.empty and not dry_run:
        print(f"\n✅ Sucesso! Arquivo limpo salvo em: {ARQUIVO_RESULTADOS}")
        print(f"   {len(df_limpo):,} registros finais")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"\n❌ ERRO: {e}")
        import traceback
        traceback.print_exc()
        input("\nPressione ENTER para sair...")
        raise
