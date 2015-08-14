import uuid
import ntpath
from simplecrypt import encrypt, decrypt


ENCRYPTION_KEY = "please change this"


class MistChunk(object):
    """Mist Chunk class"""

    CHUNK_SIZE = 50

    def __init__(self, folder_path, data):
        self.uid = uuid.uuid4()
        self.chunk_path = "%s/%s.mist" % (folder_path, self.uid)
        self._WriteToPath(data)

    def _WriteToPath(self, data):
        with open(self.chunk_path, "w") as f:
            f.write(encrypt(ENCRYPTION_KEY, data))

    def Read(self):
        with open(self.chunk_path, "r") as f:
            data = f.read()
        return decrypt(ENCRYPTION_KEY, data)


class MistFile(object):
    """Mist File class"""

    STORAGE_FOLDER_PATH = "chunks"

    def __init__(self, file_path):
        self.filename = ntpath.basename(file_path)
        self.mist_chunks = []
        self._SplitFileIntoChunks(file_path)

    def _SplitFileIntoChunks(self, file_path):
        with open(file_path, "r") as f:
            data = f.read(MistChunk.CHUNK_SIZE)
            while data:
                mist_chunk = MistChunk(MistFile.STORAGE_FOLDER_PATH, data)
                self.mist_chunks.append(mist_chunk)
                data = f.read(MistChunk.CHUNK_SIZE)

    def Read(self):
        data = ""
        for chunk in self.mist_chunks:
            data += chunk.Read()
        return data


class Mist(object):
    """Main Mist class"""

    def __init__(self):
        self.mist_files = {}

    def AddFile(self, file_path):
        self.mist_files[file_path] = MistFile(file_path)

    def ReadFile(self, file_path):
        if file_path in self.mist_files:
            return self.mist_files[file_path].Read()
        else:
            print "File not found. File path: %s" % file_path
            return None


def main():
    m = Mist()
    m.AddFile("README.md")
    m.AddFile("testfiles/index.html")
    print m.ReadFile("README.md")
    print m.ReadFile("testfiles/index.html")

if __name__ == "__main__":
    main()
