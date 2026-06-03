# alertas_config.py
"""
Configuração centralizada de rótulos de alertas.
FONTE ÚNICA DE VERDADE.
"""

TODOS_ALERTAS = [
    "CPF inválido/em branco",    
    "Matrícula duplicada",
    "Deficiência sem descrição",
    "Descrição de deficiência indevida",
    "dt_matricula alterada",
    "Matrícula retroativa",
    "Mudança de id_aluno",
    "Frequência io-iô",
    "Sem_autodeclaracao_racial",
    "Última aparição anterior a hoje",
]

# ─── Rótulos específicos para matricula_data.py ──
ALERTA_DT_MATRICULA    = "dt_matricula alterada"
ALERTA_RETROATIVA      = "Matrícula retroativa"
ALERTA_MUDANCA_ID      = "Mudança de id_aluno"
ALERTA_IOIO            = "Frequência io-iô"
ALERTA_ULTIMA_APARICAO = "Última aparição anterior a hoje"
<<<<<<< HEAD
ALERTA_DEIXOU_FREQUENTAR = "Deixou de frequentar - última aparição"#
ALERTA_TRANSFERIDO = "Transferido - última aparição"#
=======
>>>>>>> c382a788aecb11f6e1ce22bd4da61dc34a177237

ALERTAS_AUDITORIA = {
    ALERTA_DT_MATRICULA,
    ALERTA_RETROATIVA,    
    ALERTA_MUDANCA_ID,
    ALERTA_IOIO,
    ALERTA_ULTIMA_APARICAO,    
}