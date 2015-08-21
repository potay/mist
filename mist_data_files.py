import uuid
import threading
import copy

import mist_network
import mist_chunk
import mist_file


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
            mist_network_client = mist_network.MistNetworkClient(self.mist_network_address)
            (self.mist_network_member_uid, self.data_uid) = mist_network_client.StoreDataOnNetwork(data)
            self._data = None

    def Read(self):
        if self._creation_thread:
            self._creation_thread.join(MistDataFile.DEFAULT_READ_TIMEOUT)
            if self._creation_thread.isAlive():
                print "Network File reading has timed out as file is still being saved. Please try again later."
                return
        mist_network_client = mist_network.MistNetworkClient(self.mist_network_address)
        return mist_network_client.RetrieveDataOnNetwork(self.mist_network_member_uid, str(self.data_uid))

    def Delete(self):
        if self._creation_thread:
            self._creation_thread.join(MistDataFile.DEFAULT_READ_TIMEOUT)
            if self._creation_thread.isAlive():
                print "Network File deleting has timed out as file is still being saved. Please try again later."
                return False
        mist_network_client = mist_network.MistNetworkClient(self.mist_network_address)
        return mist_network_client.DeleteDataOnNetwork(self.mist_network_member_uid, str(self.data_uid))

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


class MistDataFile(mist_chunk.MistChunk):
    """Mist Data File class"""

    STORAGE_FOLDER_PATH = "chunk"
    MAX_DATA_FILE_SPLIT_NUM = 10
    DEFAULT_READ_TIMEOUT = 10 * 60

    def __init__(self, data, mist_network_address, root_path):
        self.uid = uuid.uuid4()
        self.root_path = root_path
        self.size = len(data)
        self.mist_network_address = mist_network_address
        self.mist_chunks = []
        if len(data)/mist_chunk.MistChunk.CHUNK_SIZE > MistDataFile.MAX_DATA_FILE_SPLIT_NUM:
            self._data_file_size = len(data)/MistDataFile.MAX_DATA_FILE_SPLIT_NUM
        else:
            self._data_file_size = mist_chunk.MistChunk.CHUNK_SIZE
        self._data = data
        self._creation_thread = threading.Thread(target=self._SplitFileIntoChunks, args=(self._data,))
        self._creation_thread.start()

    def _SplitFileIntoChunks(self, data):
        if self.size > mist_chunk.MistChunk.CHUNK_SIZE:
            data = self._data
            tmp_size = 0
            for i in xrange(0, self.size, self._data_file_size):
                data_part = data[i:i+self._data_file_size]
                self._data = self._data[self._data_file_size:]
                chunk = MistNetworkDataFile(data_part, self.mist_network_address)
                self.mist_chunks.append(chunk)
                tmp_size += chunk.size
        else:
            chunk = mist_chunk.MistChunk(self.uid, mist_file.MistFile.STORAGE_FOLDER_PATH, data, self.root_path)
            self.mist_chunks.append(chunk)
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
                data_chunk = chunk.Read()
                if data_chunk is None or len(data_chunk) != chunk.size:
                    print "Unable to read data file."
                    return
                data += data_chunk
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
