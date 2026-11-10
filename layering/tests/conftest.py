"""pytest 公共夹具：确保从任意 cwd 跑都能 import 仓库根的 layering 包。"""

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))
