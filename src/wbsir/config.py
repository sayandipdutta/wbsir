from pathlib import Path
from typing import Final

THIS_PATH = Path(__file__).parents[2]

BASE_URL: Final[str] = "https://ceowestbengal.wb.gov.in"
DATABASE_PATH: Final[Path] = THIS_PATH / "wbsir.db"
DATA_DIR: Final[Path] = THIS_PATH / "data_test"
