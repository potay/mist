import os
import re
import shutil

for f in os.listdir("."):
    if re.search(".*_network_state", f):
        os.remove(f)
    if re.search(".*.pyc", f):
        os.remove(f)

for f in os.listdir("accounts"):
    if os.path.isdir(os.path.join("accounts", f)):
        shutil.rmtree(os.path.join("accounts", f))
    else:
        os.remove(os.path.join("accounts", f))
