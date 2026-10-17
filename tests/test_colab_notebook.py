"""Static validation for the official one-cell Colab launcher."""

import ast
import json
from pathlib import Path


def test_official_colab_notebook_has_one_valid_code_cell():
    notebook_path = Path(__file__).resolve().parents[1] / "colab" / "TelegramDriveBot_OneCell.ipynb"
    notebook = json.loads(notebook_path.read_text(encoding="utf-8"))

    assert notebook["nbformat"] == 4
    code_cells = [cell for cell in notebook["cells"] if cell.get("cell_type") == "code"]
    assert len(code_cells) == 1

    source = "".join(code_cells[0]["source"])
    ast.parse(source, filename=str(notebook_path))
    assert 'userdata.get("TELEGRAM_BOT_TOKEN")' in source
    assert 'userdata.get("OWNER_ID")' in source
    assert "ENGINE_COMMIT = " in source
    assert "drive.mount(\"/content/drive\")" in source
