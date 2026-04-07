import os
from pathlib import Path
import sys


os.environ["DATABASE_URL"] = "sqlite:///:memory:"


API_ROOT = Path(__file__).resolve().parents[1]

if str(API_ROOT) not in sys.path:
    sys.path.insert(0, str(API_ROOT))
