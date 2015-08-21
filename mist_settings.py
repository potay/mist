import hashlib


PASSWORD = "ChangeThisPlease"
ENCRYPTION_KEY = hashlib.sha256(PASSWORD).digest()
