from pathlib import Path
import sys


ETL_ROOT = Path(__file__).resolve().parents[1]

if str(ETL_ROOT) not in sys.path:
    sys.path.insert(0, str(ETL_ROOT))
