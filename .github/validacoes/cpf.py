import pandas as pd

def limpar_cpf(cpf: str) -> str:
    """Remove pontos e traço, retorna só os dígitos com zfill(11)."""
    return str(cpf).strip().replace(".", "").replace("-", "").zfill(11)

def validar_cpf(cpf: str) -> bool:
    cpf = limpar_cpf(cpf)
    if len(cpf) != 11 or not cpf.isdigit() or cpf == cpf[0] * 11:
        return False
    for i in range(2):
        soma = sum(int(cpf[j]) * (10 + i - j) for j in range(9 + i))
        digito = (soma * 10 % 11) % 10
        if digito != int(cpf[9 + i]):
            return False
    return True

def checar_cpf(df: pd.DataFrame) -> pd.DataFrame:
    cpf_limpo = df["cpf"].apply(limpar_cpf)
    em_branco = df["cpf"].isna() | (df["cpf"].str.strip() == "") | (cpf_limpo == "00000000000")
    invalido = ~cpf_limpo.apply(validar_cpf) & ~em_branco
    erros = df[em_branco | invalido].copy()
    erros["alerta"] = "CPF inválido/em branco"
    erros["detalhe"] = cpf_limpo[erros.index].apply(
        lambda x: "Em branco" if x in ["", "00000000000"] else "CPF inválido"
    )
    return erros