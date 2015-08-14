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

    def __init__(self, file_uid, folder_path, data, network_member):
        self.file_uid = file_uid
        self.uid = uuid.uuid4()
        self.network_member = network_member
        self.chunk_path = "%s/%s/%s.mist" % (network_member.root_path, folder_path, self.uid)
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

    def __init__(self, file_path, mist_network):
        self.uid = uuid.uuid4()
        self.mist_network = mist_network
        self.filename = ntpath.basename(file_path)
        self.mist_chunks = []
        self._SplitFileIntoChunks(file_path)

    def _SplitFileIntoChunks(self, file_path):
        with open(file_path, "r") as infile:
            data = infile.read(MistChunk.CHUNK_SIZE)
            while data:
                chosen_member = self.mist_network.GetRandomMember()
                mist_chunk = MistChunk(self.uid, MistFile.STORAGE_FOLDER_PATH, data, chosen_member)
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
        self.mist_network = None
        self.filename = None
        self.mist_chunks = None

    def __str__(self):
        return self.filename


class Mist(object):
    """Main Mist class"""

    DEFAULT_INDEX_FILENAME = "index"

    def __init__(self, root_path):
        self.uid = uuid.uuid4()
        self.mist_network = None
        self.mist_network_member_uid = None
        self.root_path = root_path
        self.mist_files = {}
        self._LoadMistFiles()

    @staticmethod
    def CreateIfDoesNotExist(root_path):
        if not os.path.exists(root_path):
            os.makedirs(root_path)
        return Mist(root_path)

    def _LoadMistFiles(self):
        if os.path.isfile(os.path.join(self.root_path, Mist.DEFAULT_INDEX_FILENAME)):
            with open(os.path.join(self.root_path, Mist.DEFAULT_INDEX_FILENAME), "r") as infile:
                self.mist_files = pickle.load(infile)

    def _RewriteMistIndexFile(self):
        with open(os.path.join(self.root_path, Mist.DEFAULT_INDEX_FILENAME), "wb") as outfile:
            pickle.dump(self.mist_files, outfile)

    def AddFile(self, file_path, overwrite=True):
        if file_path in self.mist_files:
            if overwrite:
                self.DeleteFile(file_path)
            else:
                print "File already exists. File path: %s" % file_path
                return

        self.mist_files[file_path] = MistFile(file_path, self.mist_network)
        self._RewriteMistIndexFile()

    def ReadFile(self, file_path):
        if file_path in self.mist_files:
            return self.mist_files[file_path].Read()
        else:
            print "File not found. File path: %s" % file_path
            return None

    def DeleteFile(self, file_path):
        self.mist_files[file_path].Delete()
        del self.mist_files[file_path]
        self._RewriteMistIndexFile()

    def ExportFile(self, file_path, export_path):
        data = self.ReadFile(file_path)
        with open(export_path, "wb") as outfile:
            outfile.write(data)

    def List(self):
        return map(str, self.mist_files)

    def JoinNetwork(self, mist_network):
        self.mist_network = mist_network
        network_member = MistNetworkMember(self.uid, self.root_path)
        self.mist_network_member_uid = network_member.uid
        self.mist_network.AddMember(network_member)

    def LeaveNetwork(self, mist_network):
        self.mist_network.DeleteMember(self.mist_network_member_uid)
        self.mist_network = None
        self.mist_network_member_uid = None


class MistNetworkMember(object):
    """Mist Network Member Class"""

    def __init__(self, mist_uid, root_path):
        self.uid = uuid.uuid4()
        self.mist_uid = mist_uid
        self.root_path = root_path

    def __str__(self):
        return "%s@%s" % (self.mist_uid, self.root_path)


class MistNetwork(object):
    """Mist Network Class"""

    DEFAULT_NETWORK_STATE_FILENAME = "network_state"

    def __init__(self):
        self.network_members = {}
        self._LoadMistNetworkState()

    def _LoadMistNetworkState(self):
        if os.path.isfile(MistNetwork.DEFAULT_NETWORK_STATE_FILENAME):
            with open(MistNetwork.DEFAULT_NETWORK_STATE_FILENAME, "r") as infile:
                self.network_members = pickle.load(infile)

    def _RewriteNetworkStateFile(self):
        with open(MistNetwork.DEFAULT_NETWORK_STATE_FILENAME, "wb") as outfile:
            pickle.dump(self.network_members, outfile)

    def AddMember(self, member):
        self.network_members[member.uid] = member
        self._RewriteNetworkStateFile()

    def DeleteMember(self, member_uid):
        del self.network_members[member_uid]
        self._RewriteNetworkStateFile()

    def GetRandomMember(self):
        return random.choice(self.network_members.values())

    def ListMembers(self):
        return map(str, self.network_members.values())


def CheckFile(file_path):
    m = Mist()
    with open(file_path, "r") as infile:
        file_data = infile.read()
    m.AddFile(file_path)
    read_data = m.ReadFile(file_path)
    return file_data == read_data


def main():
    network = MistNetwork()
    print "network:", network.ListMembers()
    m1 = Mist.CreateIfDoesNotExist("accounts/a")
    m1.JoinNetwork(network)
    m2 = Mist.CreateIfDoesNotExist("accounts/b")
    m2.JoinNetwork(network)
    m3 = Mist.CreateIfDoesNotExist("accounts/c")
    m3.JoinNetwork(network)
    print "network:", network.ListMembers()
    print "a:", m1.List()
    print "b:", m2.List()
    print "c:", m3.List()
    import pdb; pdb.set_trace()
    print "a:", m1.List()
    print "b:", m2.List()
    print "c:", m3.List()
    print "network:", network.ListMembers()
    m1.LeaveNetwork(network)
    m2.LeaveNetwork(network)
    m3.LeaveNetwork(network)
    print "network:", network.ListMembers()


if __name__ == "__main__":
    main()
