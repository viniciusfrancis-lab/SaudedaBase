# validacoes/__init__.py
"""
Módulo de validações do Painel SEDU

Consolidador de todas as funções de validação e auditoria.
"""

# Importar todas as funções de validação
try:
    from .cpf import checar_cpf
except ImportError:
    pass

try:
    from .turma import checar_sem_turma
except ImportError:
    pass

try:
    from .matricula import checar_duplicatas
except ImportError:
    pass

try:
    from .cor_raca import checar_sem_autodeclaracao_racial
except ImportError:
    pass

try:
    from .campos import checar_campos_vazios
except ImportError:
    pass

try:
    from .flag_deficiencia import checar_flag_deficiencia
except ImportError:
    pass

try:
    from .matricula_data import checar_alteracao_dt_matricula
except ImportError:
    pass

# Importar funções de resumo de métricas
try:
    from .resumo_metricas import (
        gerar_resumo_metricas,
        exibir_tabela_metricas,
        exportar_metricas_excel,
        consolidar_alertas,
    )
except ImportError:
    pass

__all__ = [
    # Validações simples
    'checar_cpf',
    'checar_sem_turma',
    'checar_duplicatas',
    'checar_sem_autodeclaracao_racial',
    'checar_campos_vazios',
    'checar_flag_deficiencia',
    'checar_alteracao_dt_matricula',
    # Resumo e consolidação
    'gerar_resumo_metricas',
    'exibir_tabela_metricas',
    'exportar_metricas_excel',
    'consolidar_alertas',
]
