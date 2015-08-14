import uuid
import ntpath
import random
import struct
import pickle
import os
from Crypto.Cipher import AES


ENCRYPTION_KEY = "PleaseChangeThis"


class MistChunk(object):
    """Mist Chunk class"""

    CHUNK_SIZE = 512*1000
    HEADER_INFO = ((16, "File UID"),
                   (16, "Chunk UID"),
                   (8, "File size"),
                   (16, "Initialization Vector"))

    def __init__(self, file_uid, folder_path, data):
        self.file_uid = file_uid
        self.uid = uuid.uuid4()
        self.chunk_path = "%s/%s.mist" % (folder_path, self.uid)
        self._WriteToPath(data)

    def _WriteToPath(self, data):
        iv = "".join(chr(random.randint(0, 0xFF)) for i in range(16))
        encryptor = AES.new(ENCRYPTION_KEY, AES.MODE_CBC, iv)
        filesize = len(data)

        with open(self.chunk_path, "wb") as outfile:
            outfile.write(self.file_uid.bytes_le)
            outfile.write(self.uid.bytes_le)
            outfile.write(struct.pack("<Q", filesize))
            outfile.write(iv)

            if len(data) % 16 != 0:
                data += " " * (16 - len(data) % 16)
            outfile.write(encryptor.encrypt(data))

    def Read(self):
        if os.path.isfile(self.chunk_path):
            with open(self.chunk_path, "rb") as infile:
                file_uid = uuid.UUID(bytes_le=infile.read(16))
                if file_uid != self.file_uid:
                    print "ERROR", uuid
                    return

                uid = uuid.UUID(bytes_le=infile.read(16))
                if uid != self.uid:
                    print "ERROR", uuid
                    return

                original_size = struct.unpack("<Q", infile.read(struct.calcsize("Q")))[0]
                iv = infile.read(16)
                decryptor = AES.new(ENCRYPTION_KEY, AES.MODE_CBC, iv)
                encrypted_data = infile.read(MistChunk.CHUNK_SIZE)

            return decryptor.decrypt(encrypted_data)[:original_size]
        else:
            print "Chunk is invalid."

    def Delete(self):
        os.remove(self.chunk_path)
        self.file_uid = None
        self.uid = None
        self.chunk_path = None


class MistFile(object):
    """Mist File class"""

    STORAGE_FOLDER_PATH = "chunks"

    def __init__(self, file_path):
        self.uid = uuid.uuid4()
        self.filename = ntpath.basename(file_path)
        self.mist_chunks = []
        self._SplitFileIntoChunks(file_path)

    def _SplitFileIntoChunks(self, file_path):
        with open(file_path, "r") as infile:
            data = infile.read(MistChunk.CHUNK_SIZE)
            while data:
                mist_chunk = MistChunk(self.uid, MistFile.STORAGE_FOLDER_PATH, data)
                self.mist_chunks.append(mist_chunk)
                data = infile.read(MistChunk.CHUNK_SIZE)

    def Read(self):
        if self.uid:
            data = ""
            for chunk in self.mist_chunks:
                data += chunk.Read()
            return data
        else:
            print "File is invalid."

    def Delete(self):
        for chunk in self.mist_chunks:
            chunk.Delete()
            del chunk
        self.uid = None
        self.filename = None
        self.mist_chunks = None

    def __str__(self):
        return self.filename


class Mist(object):
    """Main Mist class"""

    DEFAULT_INDEX_FILENAME = "index"

    def __init__(self):
        self.mist_files = {}
        self._LoadMistFiles()

    def _LoadMistFiles(self):
        if os.path.isfile(Mist.DEFAULT_INDEX_FILENAME):
            with open(Mist.DEFAULT_INDEX_FILENAME, "r") as infile:
                self.mist_files = pickle.load(infile)

    def AddFile(self, file_path, overwrite=True):
        if file_path in self.mist_files:
            if overwrite:
                self.DeleteFile(file_path)
            else:
                print "File already exists. File path: %s" % file_path
                return

        self.mist_files[file_path] = MistFile(file_path)
        with open(Mist.DEFAULT_INDEX_FILENAME, "wb") as outfile:
            pickle.dump(self.mist_files, outfile)

    def ReadFile(self, file_path):
        if file_path in self.mist_files:
            return self.mist_files[file_path].Read()
        else:
            print "File not found. File path: %s" % file_path
            return None

    def DeleteFile(self, file_path):
        self.mist_files[file_path].Delete()
        del self.mist_files[file_path]

    def ExportFile(self, file_path, export_path):
        data = self.ReadFile(file_path)
        with open(export_path, "wb") as outfile:
            outfile.write(data)

    def List(self):
        return map(str, self.mist_files)


def CheckFile(file_path):
    m = Mist()
    with open(file_path, "r") as infile:
        file_data = infile.read()
    m.AddFile(file_path)
    read_data = m.ReadFile(file_path)
    return file_data == read_data


def main():
    m = Mist()
    print m.List()
    import pdb; pdb.set_trace()
    print m.List()

if __name__ == "__main__":
    main()
