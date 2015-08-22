import uuid
import ntpath
import random
import struct
import logging
from Crypto.Cipher import AES

import data_files
import settings


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class MistFile(object):
    """Mist File class"""

    STORAGE_FOLDER_PATH = "chunks"

    def __init__(self, file_path, mist_network_address):
        self.uid = uuid.uuid4()
        self.mist_network_address = mist_network_address
        self.filename = ntpath.basename(file_path)
        self.size = None
        self.mist_network_data_file = None
        self._MakeDataFile(file_path)

    def _MakeDataFile(self, file_path):
        with open(file_path, "r") as infile:
            data = infile.read()
            iv = "".join(chr(random.randint(0, 0xFF)) for i in range(16))
            encryptor = AES.new(settings.ENCRYPTION_KEY, AES.MODE_CBC, iv)
            self.size = len(data)

            encrypted_data = struct.pack("<Q", self.size)
            encrypted_data += iv
            if len(data) % 16 != 0:
                    data += " " * (16 - len(data) % 16)
            encrypted_data += encryptor.encrypt(data)
            mist_data_file = data_files.MistNetworkDataFile(encrypted_data, self.mist_network_address)
            self.mist_network_data_file = mist_data_file

    def Read(self):
        if self.uid:
            encrypted_data = self.mist_network_data_file.Read()
            if encrypted_data is None:
                logger.error("Unable to read file %s", self.filename)
                return None
            original_size = struct.unpack("<Q", encrypted_data[:struct.calcsize("Q")])[0]
            encrypted_data = encrypted_data[struct.calcsize("Q"):]
            if self.size != original_size:
                logger.error("Corrupted file due to size. Size on record: %s, Size in header: %s", self.size, original_size)
                return None
            iv = encrypted_data[:16]
            encrypted_data = encrypted_data[16:]
            decryptor = AES.new(settings.ENCRYPTION_KEY, AES.MODE_CBC, iv)

            data = decryptor.decrypt(encrypted_data)[:original_size]
            if self.size != len(data):
                logger.error("Corrupted file due to size. Size on record: %s, Actual size: %s", self.size, len(data))
                return None
            else:
                return data
        else:
            logger.error("File is invalid.")
            return None

    def Delete(self):
        if self.uid:
            if self.mist_network_data_file.Delete():
                self.uid = None
                self.mist_network = None
                self.filename = None
                self.mist_network_data_file = None
            else:
                return False
        return True

    def __str__(self):
        return self.filename
