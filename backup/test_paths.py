from pathlib import Path

import pdb

pdb.set_trace()
filename = Path(__file__)
print(filename)
frontend_dir = Path(filename.parents[1] / "hermes" / "reporting" / "frontend")

print(frontend_dir)
