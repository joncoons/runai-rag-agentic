"""Run portable unit suites; integration suites require explicit external services."""
import sys,unittest,tempfile
from test_fixtures import prepare
from pathlib import Path
root=Path(__file__).resolve().parents[1];sys.path.insert(0,str(root));sys.path.insert(0,str(root/"runtime/src"))
temporary=tempfile.TemporaryDirectory(prefix="rag-unit-fixtures-")
prepare(root,temporary.name)
suite=unittest.TestSuite()
for folder in [root/"tests",*[p for p in sorted((root/"tests").iterdir()) if p.is_dir() and p.name not in {"__pycache__","ui","integration"}]]:
 suite.addTests(unittest.defaultTestLoader.discover(str(folder),pattern="test*.py"))
result=unittest.TextTestRunner(verbosity=1).run(suite);sys.exit(not result.wasSuccessful())
