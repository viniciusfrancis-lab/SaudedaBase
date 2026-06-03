import re

arquivos = [
    'alertas_config.py', 'app.py',
    'empilhar/atualizacao.py', 'empilhar/geracao.py',
    'validacoes/campos.py', 'validacoes/cpf.py',
    'validacoes/dados_grafico.py', 'validacoes/matricula.py',
    'validacoes/matricula_data.py'
]

for path in arquivos:
    content = open(path, 'r', encoding='utf-8').read()
    cleaned = re.sub(r'<<<<<<< HEAD\n(.*?)=======.*?>>>>>>>[^\n]*\n', r'\1', content, flags=re.DOTALL)
    open(path, 'w', encoding='utf-8').write(cleaned)
    print('OK:', path)