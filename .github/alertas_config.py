# alertas_config.py
"""
Configuração centralizada de rótulos de alertas.
Sincroniza os nomes entre app.py e matricula_data.py
"""

# ─── Rótulos de alertas (FONTE ÚNICA DE VERDADE) ──
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
    
]

# ─── Rótulos específicos para matricula_data.py ──
ALERTA_DT_MATRICULA = "dt_matricula alterada"
ALERTA_RETROATIVA   = "Matrícula retroativa"
ALERTA_MUDANCA_ID   = "Mudança de id_aluno"
ALERTA_IOIO         = "Frequência io-iô"


# ─── Conjunto para auditoria temporal ──
ALERTAS_AUDITORIA = {
    ALERTA_DT_MATRICULA,
    ALERTA_RETROATIVA,    
    ALERTA_MUDANCA_ID,
    ALERTA_IOIO,    
}
