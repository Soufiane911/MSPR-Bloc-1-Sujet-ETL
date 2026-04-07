"""
Pytest configuration and fixtures for ObRail Europe tests.
"""

import sys
from pathlib import Path

# Add the root directory to Python path so modules can be imported
root_dir = Path(__file__).parent.parent
etl_dir = root_dir / 'etl'
api_dir = root_dir / 'api'

sys.path.insert(0, str(root_dir))
sys.path.insert(0, str(etl_dir))
sys.path.insert(0, str(api_dir))
