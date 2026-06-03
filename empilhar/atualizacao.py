import os
import pandas as pd
from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq
import logging
from datetime import datetime
from pygei.seges import alunos

# 1. Configuração do Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(), # Mostra no terminal
        # logging.FileHandler('processamento_matriculas.log') # Opcional: salva em arquivo
    ]
)
logger = logging.getLogger(__name__)

SAVE_FOLDER = Path(__file__).parent.parent / "dados"
NUM_ANO_LETIVO = '2026'

# 