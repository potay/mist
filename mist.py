import uuid
import ntpath
import random
import struct
import pickle
import os
import threading
import copy
from Crypto.Cipher import AES

import mist_network
import mist_watchdog

import hashlib

PASSWORD = "ChangeThisPlease"
ENCRYPTION_KEY = hashlib.sha256(PASSWORD).digest()


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
        self.size = None
        self.chunk_path = "%s/%s/%s.mist" % (root_path, folder_path, self.uid)
        self._WriteToPath(data)

    def _WriteToPath(self, data):
        iv = "".join(chr(random.randint(0, 0xFF)) for i in range(16))
        encryptor = AES.new(ENCRYPTION_KEY, AES.MODE_CBC, iv)
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
                    print "ERROR", uuid
                    return

                uid = uuid.UUID(bytes_le=infile.read(16))
                if uid != self.uid:
                    print "ERROR", uuid
                    return

                original_size = struct.unpack("<Q", infile.read(struct.calcsize("Q")))[0]
                if self.size != original_size:
                    print "ERROR: Corrupted file due to filesize."
                    return
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
        return True


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
            encryptor = AES.new(ENCRYPTION_KEY, AES.MODE_CBC, iv)
            self.size = len(data)

            encrypted_data = struct.pack("<Q", self.size)
            encrypted_data += iv
            if len(data) % 16 != 0:
                    data += " " * (16 - len(data) % 16)
            encrypted_data += encryptor.encrypt(data)
            mist_data_file = MistNetworkDataFile(encrypted_data, self.mist_network_address)
            self.mist_network_data_file = mist_data_file

    def Read(self):
        if self.uid:
            encrypted_data = self.mist_network_data_file.Read()
            if encrypted_data is None:
                print "Unable to read file %s" % self.filename
                return
            original_size = struct.unpack("<Q", encrypted_data[:struct.calcsize("Q")])[0]
            encrypted_data = encrypted_data[struct.calcsize("Q"):]
            if self.size != original_size:
                print "Corrupted file due to size. Size on record: %s, Size in header: %s" % (self.size, original_size)
                return
            iv = encrypted_data[:16]
            encrypted_data = encrypted_data[16:]
            if self.size > len(encrypted_data):
                print "Corrupted file due to size. Size on record: %s, Actual size: %s" % (self.size, len(encrypted_data))
                return
            decryptor = AES.new(ENCRYPTION_KEY, AES.MODE_CBC, iv)

            data = decryptor.decrypt(encrypted_data)[:original_size]
            return data
        else:
            print "File is invalid."

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


class MistNetworkDataFile(object):

    def __init__(self, data, mist_network_address):
        self.mist_network_address = mist_network_address
        self.mist_network_member_uid = None
        self.data_uid = None
        self.size = len(data)
        self._data = data
        self._creation_thread = threading.Thread(target=self._StoreDataFileOnNetwork, args=(self._data,))
        self._creation_thread.start()

    def _StoreDataFileOnNetwork(self, data):
        if not self.data_uid:
            # iv = "".join(chr(random.randint(0, 0xFF)) for i in range(16))
            # encryptor = AES.new(ENCRYPTION_KEY, AES.MODE_CBC, iv)
            # filesize = len(data)

            # encrypted_data = struct.pack("<Q", filesize)
            # encrypted_data += iv
            # if len(data) % 16 != 0:
            #         data += " " * (16 - len(data) % 16)
            # encrypted_data += encryptor.encrypt(data)

            data_base64 = data.encode("base64", "strict")

            mist_network_client = mist_network.MistNetworkClient(self.mist_network_address)
            response = mist_network_client.store(data_base64, "base64")
            self.mist_network_member_uid = response["network_member_uid"]
            self.data_uid = uuid.UUID(response["data_uid"])
            self._data = None

    def Read(self):
        if self._creation_thread:
            self._creation_thread.join(MistDataFile.DEFAULT_READ_TIMEOUT)
            if self._creation_thread.isAlive():
                print "Network File reading has timed out as file is still being saved. Please try again later."
                return
        mist_network_client = mist_network.MistNetworkClient(self.mist_network_address)
        response = mist_network_client.retrieve(self.mist_network_member_uid, str(self.data_uid))
        if "error_message" in response:
            print "Unable to read data file. Error: %s" % response["error_message"]
            return None
        else:
            return response["data"].decode(response["encoding"])

    def Delete(self):
        if self._creation_thread:
            self._creation_thread.join(MistDataFile.DEFAULT_READ_TIMEOUT)
            if self._creation_thread.isAlive():
                print "Network File deleting has timed out as file is still being saved. Please try again later."
                return False
        mist_network_client = mist_network.MistNetworkClient(self.mist_network_address)
        response = mist_network_client.delete(self.mist_network_member_uid, str(self.data_uid))
        if response and "error_message" in response:
            print response["error_message"]
            return False
        return True

    def __getstate__(self):
        d = copy.copy(self.__dict__)
        del d["_creation_thread"]
        return d

    def __setstate__(self, d):
        d["_creation_thread"] = None
        self.__dict__ = d
        if self._data:
            self._creation_thread = threading.Thread(target=self._StoreDataFileOnNetwork, args=(self._data,))
            self._creation_thread.start()


class MistDataFile(MistChunk):
    """Mist Data File class"""

    STORAGE_FOLDER_PATH = "chunk"
    DATA_FILE_SPLIT_NUM = 10
    DEFAULT_READ_TIMEOUT = 10 * 60

    def __init__(self, data, mist_network_address, root_path):
        self.uid = uuid.uuid4()
        self.root_path = root_path
        self.size = len(data)
        self.mist_network_address = mist_network_address
        self.mist_chunks = []
        self._data_file_size = len(data)/MistDataFile.DATA_FILE_SPLIT_NUM
        self._data = data
        self._creation_thread = threading.Thread(target=self._SplitFileIntoChunks, args=(self._data,))
        self._creation_thread.start()

    def _SplitFileIntoChunks(self, data):
        if self.size > MistChunk.CHUNK_SIZE:
            data = self._data
            tmp_size = 0
            for i in xrange(0, self.size, self._data_file_size):
                data_part = data[i:i+self._data_file_size]
                self._data = self._data[self._data_file_size:]
                mist_chunk = MistNetworkDataFile(data_part, self.mist_network_address)
                self.mist_chunks.append(mist_chunk)
                tmp_size += mist_chunk.size
                print "Length of selfdata:", len(self._data)
            print "selfdata:", self._data
            print "Size on record: %s Stored size: %s" % (self.size, tmp_size)
        else:
            mist_chunk = MistChunk(self.uid, MistFile.STORAGE_FOLDER_PATH, data, self.root_path)
            print "Size on record: %s Stored size: %s" % (self.size, mist_chunk.size)
            self.mist_chunks.append(mist_chunk)
            self._data = None

    def Read(self):
        if self.uid:
            if self._creation_thread:
                self._creation_thread.join(MistDataFile.DEFAULT_READ_TIMEOUT)
                if self._creation_thread.isAlive():
                    print "Data File reading has timed out as file is still being saved. Please try again later."
                    return
            data = ""
            for chunk in self.mist_chunks:
                print "Getting data from chunk type: %s" % type(chunk)
                data_chunk = chunk.Read()
                if data_chunk is None or len(data_chunk) != chunk.size:
                    print "Unable to read data file."
                    return
                data += data_chunk
            print "Size in record: %s Actual size: %s" % (self.size, len(data))
            return data
        else:
            print "File is invalid."

    def Delete(self):
        if self._creation_thread:
            self._creation_thread.join(MistDataFile.DEFAULT_READ_TIMEOUT)
            if self._creation_thread.isAlive():
                print "Data File deleting has timed out as file is still being saved. Please try again later."
                return False
        for chunk in self.mist_chunks:
            if chunk.Delete():
                del chunk
            else:
                return False
        self.uid = None
        self.mist_network_address = None
        self.mist_chunks = None
        return True

    def __str__(self):
        return self.filename

    def __getstate__(self):
        d = copy.copy(self.__dict__)
        del d["_creation_thread"]
        return d

    def __setstate__(self, d):
        d["_creation_thread"] = None
        self.__dict__ = d
        if self._data:
            self._creation_thread = threading.Thread(target=self._SplitFileIntoChunks, args=(self._data,))
            self._creation_thread.start()


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
        self._lock = threading.Lock()

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
                (self.mist_network_member_uid, self.mist_files, self.mist_data_files) = self._PerformWithLock(pickle.load, infile)
        else:
            self._RewriteMistIndexFile()

    def _RewriteMistIndexFile(self):
        with self._lock:
            with open(os.path.join(self.root_path, Mist.DEFAULT_INDEX_FILENAME), "wb") as outfile:
                pickle.dump((self.mist_network_member_uid, self.mist_files, self.mist_data_files), outfile)

    def _PerformWithLock(self, func, *args, **kwargs):
        with self._lock:
            return func(*args, **kwargs)

    def _SetDictKeyValueWithLock(self, dictionary, key, value):
        def Set():
            dictionary[key] = value
        self._PerformWithLock(Set)

    def _DeleteDictKeyWithLock(self, dictionary, key):
        def Delete():
            del dictionary[key]
        self._PerformWithLock(Delete)

    def AddFile(self, file_path, overwrite=True):
        if file_path in self.mist_files:
            if overwrite:
                self.DeleteFile(file_path)
            else:
                print "File already exists. File path: %s" % file_path
                return

        self._SetDictKeyValueWithLock(self.mist_files, file_path, MistFile(file_path, self.mist_network_address))
        self._RewriteMistIndexFile()

    def ModifyFile(self, file_path, overwrite=True):
        if file_path in self.mist_files:
            self.DeleteFile(file_path)
            self._SetDictKeyValueWithLock(self.mist_files, file_path, MistFile(file_path, self.mist_network_address))
            self._RewriteMistIndexFile()
        else:
            print "File does not exist."

    def ReadFile(self, file_path):
        if file_path in self.mist_files:
            data = self.mist_files[file_path].Read()
            if data is None or self.mist_files[file_path].size != len(data):
                print "Unable to read file path: %s" % file_path
                return
            else:
                return data
        else:
            print "File not found. File path: %s" % file_path
            return None

    def DeleteFile(self, file_path):
        if file_path in self.mist_files:
            if self.mist_files[file_path].Delete():
                self._DeleteDictKeyWithLock(self.mist_files, file_path)
                self._RewriteMistIndexFile()
            else:
                return False
        return True

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

    def LeaveNetwork(self, local=False):
        if not local:
            self.mist_network_client.leave(str(self.mist_network_member_uid))
        self.mist_local_server.Stop()
        self.mist_local_server = None
        self.mist_local_address = None
        self.mist_network_client = None
        self.mist_network_address = None

    def StoreDataFile(self, data):
        data_file = MistDataFile(data, self.mist_network_address, self.root_path)
        self._SetDictKeyValueWithLock(self.mist_data_files, data_file.uid, data_file)
        self._RewriteMistIndexFile()
        return data_file.uid

    def RetrieveDataFile(self, data_uid):
        if data_uid in self.mist_data_files:
            data = self.mist_data_files[data_uid].Read()
            if data is None or self.mist_data_files[data_uid].size != len(data):
                print "Error in getting data file uid: %s" % data_uid
                return None
            else:
                return data
        else:
            print "Error, invalid data file uid: %s" % data_uid
            return None

    def DeleteDataFile(self, data_uid):
        if data_uid in self.mist_data_files:
            if self.mist_data_files[data_uid].Delete():
                self._DeleteDictKeyWithLock(self.mist_data_files, data_uid)
                self._RewriteMistIndexFile()
            else:
                return False
        return True


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
        except KeyboardInterrupt:
            print "Quiting..."
        finally:
            print "%s:" % args.root_path, m.List()
            m.Stop()
    except MistError as e:
        print "Error:", e


if __name__ == "__main__":
    main()
