import uuid
import random
import struct
import os
import logging
from Crypto.Cipher import AES

import settings


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class MistChunk(object):
    """Mist Chunk class"""

    CHUNK_SIZE = 512*10*1000
    HEADER_INFO = ((16, "File UID"),
                   (16, "Chunk UID"),
                   (8, "File size"),
                   (16, "Initialization Vector"))

    def __init__(self, file_uid, folder_path, data, root_path):
        self.file_uid = file_uid
        self.uid = uuid.uuid4()
        self.size = None
        self.chunk_path = "%s/%s/%s.mist" % (root_path, folder_path, self.uid)
        self._WriteToPath(data)

    def _WriteToPath(self, data):
        iv = "".join(chr(random.randint(0, 0xFF)) for i in range(16))
        encryptor = AES.new(settings.ENCRYPTION_KEY, AES.MODE_CBC, iv)
        self.size = len(data)

        with open(self.chunk_path, "wb") as outfile:
            outfile.write(self.file_uid.bytes_le)
            outfile.write(self.uid.bytes_le)
            outfile.write(struct.pack("<Q", self.size))
            outfile.write(iv)

            if len(data) % 16 != 0:
                data += " " * (16 - len(data) % 16)
            outfile.write(encryptor.encrypt(data))

    def Read(self):
        if os.path.isfile(self.chunk_path):
            with open(self.chunk_path, "rb") as infile:
                file_uid = uuid.UUID(bytes_le=infile.read(16))
                if file_uid != self.file_uid:
                    logger.error("UID: %s", uuid)
                    return

                uid = uuid.UUID(bytes_le=infile.read(16))
                if uid != self.uid:
                    logger.error("UID: %s", uuid)
                    return

                original_size = struct.unpack("<Q", infile.read(struct.calcsize("Q")))[0]
                if self.size != original_size:
                    logger.error("Corrupted file due to filesize. UID: %s", uuid)
                    return
                iv = infile.read(16)
                decryptor = AES.new(settings.ENCRYPTION_KEY, AES.MODE_CBC, iv)
                encrypted_data = infile.read(MistChunk.CHUNK_SIZE)

            return decryptor.decrypt(encrypted_data)[:original_size]
        else:
            logger.error("Chunk is invalid.")

    def Delete(self):
        os.remove(self.chunk_path)
        self.file_uid = None
        self.uid = None
        self.chunk_path = None
        return True
