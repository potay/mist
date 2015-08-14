import uuid
import ntpath
import random
import struct
import pickle
import os
import re
import time
from Crypto.Cipher import AES
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler


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

    def __init__(self, root_path, autostart=False):
        self.uid = uuid.uuid4()
        self.mist_network = None
        self.mist_network_member_uid = None
        self.root_path = root_path
        self.mist_files = {}
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
        if self.mist_network:
            self.LeaveNetwork()

    def _StartWatchdogObserver(self):
        self.event_handler = MistWatchdogEventHandler(self)
        self.observer = Observer()
        self.observer.schedule(self.event_handler, self.root_path, recursive=True)
        self.observer.start()

    def _LoadMistFiles(self):
        if os.path.isfile(os.path.join(self.root_path, Mist.DEFAULT_INDEX_FILENAME)):
            with open(os.path.join(self.root_path, Mist.DEFAULT_INDEX_FILENAME), "r") as infile:
                self.mist_files = pickle.load(infile)
        else:
            self._RewriteMistIndexFile()

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

    def LeaveNetwork(self):
        self.mist_network.DeleteMember(self.mist_network_member_uid)
        self.mist_network = None
        self.mist_network_member_uid = None


class MistWatchdogEventHandler(FileSystemEventHandler):
    """Mist hander which handles all the events captured."""

    IGNORED_PATHS = [
        ".*/.DS_Store",
    ]

    IGNORED_MIST_ROOT_PATHS = [
        (MistFile.STORAGE_FOLDER_PATH, "folder"),
        (Mist.DEFAULT_INDEX_FILENAME, "file"),
    ]

    def __init__(self, mist_parent, *args, **kwargs):
        super(MistWatchdogEventHandler, self).__init__(*args, **kwargs)
        self.mist_parent = mist_parent

    def dispatch(self, event):
        combined = "(" + ")|(".join(MistWatchdogEventHandler.IGNORED_PATHS) + ")"

        for (path, path_type) in MistWatchdogEventHandler.IGNORED_MIST_ROOT_PATHS:
            full_path = os.path.join(self.mist_parent.root_path, path)
            if path_type == "folder":
                if os.path.commonprefix([event.src_path, full_path]) == full_path:
                    return
            elif path_type == "file":
                if event.src_path == full_path:
                    return

        if not re.match(combined, event.src_path):
            super(MistWatchdogEventHandler, self).dispatch(event)

    def on_moved(self, event):
        super(MistWatchdogEventHandler, self).on_moved(event)

        what = 'directory' if event.is_directory else 'file'
        print "Moved %s: from %s to %s" % (what, event.src_path, event.dest_path)

        if not event.is_directory:
            self.mist_parent.DeleteFile(event.src_path)
            self.mist_parent.AddFile(event.dest_path)

        print "%s:" % self.mist_parent.root_path, self.mist_parent.List()

    def on_created(self, event):
        super(MistWatchdogEventHandler, self).on_created(event)

        what = 'directory' if event.is_directory else 'file'
        print "Created %s: %s" % (what, event.src_path)

        if not event.is_directory:
            self.mist_parent.AddFile(event.src_path)

        print "%s:" % self.mist_parent.root_path, self.mist_parent.List()

    def on_deleted(self, event):
        super(MistWatchdogEventHandler, self).on_deleted(event)

        what = 'directory' if event.is_directory else 'file'
        print "Deleted %s: %s" % (what, event.src_path)

        if not event.is_directory:
            self.mist_parent.DeleteFile(event.src_path)

        print "%s:" % self.mist_parent.root_path, self.mist_parent.List()

    def on_modified(self, event):
        super(MistWatchdogEventHandler, self).on_modified(event)

        what = 'directory' if event.is_directory else 'file'
        print "Modified %s: %s" % (what, event.src_path)

        if not event.is_directory:
            self.mist_parent.AddFile(event.src_path)

        print "%s:" % self.mist_parent.root_path, self.mist_parent.List()


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
    m2 = Mist.CreateIfDoesNotExist("accounts/b")
    m3 = Mist.CreateIfDoesNotExist("accounts/c")
    try:
        m1.Start(network)
        m2.Start(network)
        m3.Start(network)
        print "network:", network.ListMembers()
        print "a:", m1.List()
        print "b:", m2.List()
        print "c:", m3.List()
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            print "a:", m1.List()
            print "b:", m2.List()
            print "c:", m3.List()
            print "network:", network.ListMembers()
            m1.Stop()
            m2.Stop()
            m3.Stop()
            print "network:", network.ListMembers()
    finally:
        m1.Stop()
        m2.Stop()
        m3.Stop()


if __name__ == "__main__":
    main()
