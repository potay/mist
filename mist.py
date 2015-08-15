import uuid
import ntpath
import random
import struct
import pickle
import os
from Crypto.Cipher import AES

import mist_network
import mist_watchdog


ENCRYPTION_KEY = "PleaseChangeThis"


class MistError(Exception):
    pass


class MistChunk(object):
    """Mist Chunk class"""

    CHUNK_SIZE = 512*1000
    HEADER_INFO = ((16, "File UID"),
                   (16, "Chunk UID"),
                   (8, "File size"),
                   (16, "Initialization Vector"))

    def __init__(self, file_uid, folder_path, data, root_path):
        self.file_uid = file_uid
        self.uid = uuid.uuid4()
        self.chunk_path = "%s/%s/%s.mist" % (root_path, folder_path, self.uid)
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

    def __init__(self, file_path, mist_network_address):
        self.uid = uuid.uuid4()
        self.mist_network_address = mist_network_address
        self.filename = ntpath.basename(file_path)
        self.mist_network_data_file = None
        self._MakeDataFile(file_path)

    def _MakeDataFile(self, file_path):
        with open(file_path, "r") as infile:
            data = infile.read()
            mist_data_file = MistNetworkDataFile(data, self.mist_network_address)
            self.mist_network_data_file = mist_data_file

    def Read(self):
        if self.uid:
            return self.mist_network_data_file.Read()
        else:
            print "File is invalid."

    def Delete(self):
        if self.uid:
            self.mist_network_data_file.Delete()
            self.uid = None
            self.mist_network = None
            self.filename = None
            self.mist_network_data_file = None

    def __str__(self):
        return self.filename


class MistNetworkDataFile(object):

    def __init__(self, data, mist_network_address):
        self.mist_network_address = mist_network_address
        self.mist_network_member_uid = None
        self.data_uid = None
        self._StoreDataFileOnNetwork(data)

    def _StoreDataFileOnNetwork(self, data):
        iv = "".join(chr(random.randint(0, 0xFF)) for i in range(16))
        encryptor = AES.new(ENCRYPTION_KEY, AES.MODE_CBC, iv)
        filesize = len(data)

        encrypted_data = struct.pack("<Q", filesize)
        encrypted_data += iv
        if len(data) % 16 != 0:
                data += " " * (16 - len(data) % 16)
        encrypted_data += encryptor.encrypt(data)

        encrypted_data_base64 = encrypted_data.encode("base64", "strict")

        mist_network_client = mist_network.MistNetworkClient(self.mist_network_address)
        response = mist_network_client.store(encrypted_data_base64)
        self.mist_network_member_uid = response["network_member_uid"]
        self.data_uid = uuid.UUID(response["data_uid"])

    def Read(self):
        mist_network_client = mist_network.MistNetworkClient(self.mist_network_address)
        response = mist_network_client.retrieve(self.mist_network_member_uid, str(self.data_uid))
        encrypted_data_base64 = response["data"]
        encrypted_data = encrypted_data_base64.decode("base64", "strict")

        original_size = struct.unpack("<Q", encrypted_data[:struct.calcsize("Q")])[0]
        encrypted_data = encrypted_data[struct.calcsize("Q"):]
        iv = encrypted_data[:16]
        encrypted_data = encrypted_data[16:]
        decryptor = AES.new(ENCRYPTION_KEY, AES.MODE_CBC, iv)

        return decryptor.decrypt(encrypted_data)[:original_size]

    def Delete(self):
        mist_network_client = mist_network.MistNetworkClient(self.mist_network_address)
        response = mist_network_client.delete(self.mist_network_member_uid, str(self.data_uid))
        if response and "error_message" in response:
            print response["error_message"]


class MistDataFile(MistChunk):
    """Mist Data File class"""

    STORAGE_FOLDER_PATH = "chunk"
    DATA_FILE_SPLIT_NUM = 10

    def __init__(self, data, mist_network_address, root_path):
        self.uid = uuid.uuid4()
        self.root_path = root_path
        self.mist_network_address = mist_network_address
        self.mist_chunks = []
        self._SplitFileIntoChunks(data)

    def _SplitFileIntoChunks(self, data):
        if len(data) > MistChunk.CHUNK_SIZE:
            print len(data), MistDataFile.DATA_FILE_SPLIT_NUM
            data_file_size = len(data)/MistDataFile.DATA_FILE_SPLIT_NUM
            print data_file_size
            for i in xrange(0, len(data), data_file_size):
                data_part = data[i:i+data_file_size]
                mist_chunk = MistNetworkDataFile(data_part, self.mist_network_address)
                self.mist_chunks.append(mist_chunk)
        else:
            mist_chunk = MistChunk(self.uid, MistFile.STORAGE_FOLDER_PATH, data, self.root_path)
            self.mist_chunks.append(mist_chunk)

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
        self.mist_network_address = None
        self.mist_chunks = None

    def __str__(self):
        return self.filename


class Mist(object):
    """Main Mist class"""

    DEFAULT_INDEX_FILENAME = "index"

    def __init__(self, root_path, autostart=False):
        self.uid = uuid.uuid4()
        self.mist_network_address = None
        self.mist_network_member_uid = None
        self.mist_local_address = None
        self.mist_local_server = None
        self.root_path = root_path
        self.mist_files = {}
        self.mist_data_files = {}
        self.event_handler = None
        self.observer = None

        if autostart:
            self.Start()

    @staticmethod
    def CreateIfDoesNotExist(root_path):
        if not os.path.exists(root_path):
            os.makedirs(root_path)
        if not os.path.exists(os.path.join(root_path, MistFile.STORAGE_FOLDER_PATH)):
            os.makedirs(os.path.join(root_path, MistFile.STORAGE_FOLDER_PATH))
        return Mist(root_path)

    def Start(self, network=None):
        self._LoadMistFiles()
        self._StartWatchdogObserver()
        if network:
            self.JoinNetwork(network)

    def Stop(self):
        if self.observer:
            self.observer.stop()
            self.observer.join()
        self._RewriteMistIndexFile()
        if self.mist_network_address:
            self.LeaveNetwork()

    def _StartWatchdogObserver(self):
        self.event_handler = mist_watchdog.MistWatchdogEventHandler(self)
        self.observer = mist_watchdog.MistWatchdogObserver()
        self.observer.schedule(self.event_handler, self.root_path, recursive=True)
        self.observer.start()

    def _LoadMistFiles(self):
        if os.path.isfile(os.path.join(self.root_path, Mist.DEFAULT_INDEX_FILENAME)):
            with open(os.path.join(self.root_path, Mist.DEFAULT_INDEX_FILENAME), "r") as infile:
                (self.mist_network_member_uid, self.mist_files, self.mist_data_files) = pickle.load(infile)
        else:
            self._RewriteMistIndexFile()

    def _RewriteMistIndexFile(self):
        with open(os.path.join(self.root_path, Mist.DEFAULT_INDEX_FILENAME), "wb") as outfile:
            pickle.dump((self.mist_network_member_uid, self.mist_files, self.mist_data_files), outfile)

    def AddFile(self, file_path, overwrite=True):
        if file_path in self.mist_files:
            if overwrite:
                self.DeleteFile(file_path)
            else:
                print "File already exists. File path: %s" % file_path
                return

        self.mist_files[file_path] = MistFile(file_path, self.mist_network_address)
        self._RewriteMistIndexFile()

    def ModifyFile(self, file_path, overwrite=True):
        if file_path in self.mist_files:
            self.DeleteFile(file_path)
            self.mist_files[file_path] = MistFile(file_path, self.mist_network_address)
            self._RewriteMistIndexFile()
        else:
            print "File does not exist."

    def ReadFile(self, file_path):
        if file_path in self.mist_files:
            return self.mist_files[file_path].Read()
        else:
            print "File not found. File path: %s" % file_path
            return None

    def DeleteFile(self, file_path):
        if file_path in self.mist_files:
            self.mist_files[file_path].Delete()
            del self.mist_files[file_path]
            self._RewriteMistIndexFile()

    def ExportFile(self, file_path, export_path):
        data = self.ReadFile(file_path)
        with open(export_path, "wb") as outfile:
            outfile.write(data)

    def List(self):
        return map(str, self.mist_files)

    def JoinNetwork(self, mist_network_address):
        self.mist_network_address = mist_network_address
        self.mist_network_client = mist_network.MistNetworkClient(self.mist_network_address)
        self.mist_local_server = mist_network.MistNetworkMemberServer(self, "localhost")
        self.mist_local_server.Start()
        self.mist_local_address = "http://%s:%d" % self.mist_local_server.server_address
        try:
            if self.mist_network_member_uid:
                response = self.mist_network_client.join(self.mist_local_address, str(self.mist_network_member_uid))
            else:
                response = self.mist_network_client.join(self.mist_local_address)
            self.mist_network_member_uid = uuid.UUID(response["network_member_uid"])
            self._RewriteMistIndexFile()
        except:
            self.mist_local_server.Stop()
            raise MistError("Could not join network. Network address: %s" % mist_network_address)

    def LeaveNetwork(self):
        self.mist_network_client.leave(str(self.mist_network_member_uid))
        self.mist_local_server.Stop()
        self.mist_local_server = None
        self.mist_local_address = None
        self.mist_network_client = None
        self.mist_network_address = None

    def StoreDataFile(self, data):
        data_file = MistDataFile(data, self.mist_network_address, self.root_path)
        self.mist_data_files[data_file.uid] = data_file
        self._RewriteMistIndexFile()
        return data_file.uid

    def RetrieveDataFile(self, data_uid):
        if data_uid in self.mist_data_files:
            return self.mist_data_files[data_uid].Read()
        else:
            return None

    def DeleteDataFile(self, data_uid):
        if data_uid in self.mist_data_files:
            self.mist_data_files[data_uid].Delete()
            del self.mist_data_files[data_uid]
            self._RewriteMistIndexFile()


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("root_path")
    args = parser.parse_args()

    m = Mist.CreateIfDoesNotExist(os.path.join("accounts", args.root_path))
    try:
        m.Start("http://localhost:15112")
        try:
            while True:
                print "%s:" % args.root_path, m.List()
                command = raw_input("What do you want to do? (Enter the action number):\n  1.Read File\n  2.Export File\n\nAction Number: ")
                if command == "1":
                    file_path = raw_input("Which file? (e.g. accounts/a/filename.txt): ")
                    print m.ReadFile(file_path)
                elif command == "2":
                    file_path = raw_input("Which file? (e.g. accounts/a/filename.txt): ")
                    output_path = raw_input("Export path? (e.g. hello.txt): ")
                    m.ExportFile(file_path, output_path)
                print
                print
        finally:
            print "%s:" % args.root_path, m.List()
            m.Stop()
    except MistError as e:
        print "ERRORRROROROROR", e


if __name__ == "__main__":
    main()
