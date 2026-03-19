
import sys
from pathlib import Path

root_dir = Path(__file__).parent.parent
etl_dir = root_dir / 'etl'
api_dir = root_dir / 'api'

sys.path.insert(0, str(root_dir))
sys.path.insert(0, str(etl_dir))
sys.path.insert(0, str(api_dir))
