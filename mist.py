import uuid
import pickle
import os
import threading
import logging

import network
import network_member
import mist_watchdog
import files
import data_files


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class MistError(Exception):
    pass


class Mist(object):
    """Main Mist class"""

    DEFAULT_INDEX_FILENAME = "index"

    def __init__(self, root_path, autostart=False):
        self.uid = uuid.uuid4()
        self.mist_network_address = None
        self.mist_network_member_uid = None
        self.mist_network_client = None
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
        if not os.path.exists(os.path.join(root_path, files.MistFile.STORAGE_FOLDER_PATH)):
            os.makedirs(os.path.join(root_path, files.MistFile.STORAGE_FOLDER_PATH))
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

    def RefreshIndex(self):
        self._RewriteMistIndexFile()

    def AddFile(self, file_path, overwrite=True):
        if file_path in self.mist_files:
            if overwrite:
                self.DeleteFile(file_path)
            else:
                logger.error("File already exists. File path: %s", file_path)
                return

        self._SetDictKeyValueWithLock(self.mist_files, file_path, files.MistFile(file_path, self.mist_network_address))
        self._RewriteMistIndexFile()

    def ModifyFile(self, file_path, overwrite=True):
        if file_path in self.mist_files:
            self.DeleteFile(file_path)
            self._SetDictKeyValueWithLock(self.mist_files, file_path, files.MistFile(file_path, self.mist_network_address))
            self._RewriteMistIndexFile()
        else:
            logger.error("File does not exist. File path: %s", file_path)

    def ReadFile(self, file_path):
        if file_path in self.mist_files:
            data = self.mist_files[file_path].Read()
            if data is None:
                logger.error("Unable to read file path: %s", file_path)
                return
            else:
                return data
        else:
            logger.warning("File not found. File path: %s", file_path)
            return None
        self._RewriteMistIndexFile()

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
        self.mist_network_client = network.MistNetworkClient(self.mist_network_address)
        self.mist_local_server = network_member.MistNetworkMemberServer(self, "localhost")
        self.mist_local_server.Start()
        self.mist_local_address = "http://%s:%d" % self.mist_local_server.server_address
        uid = self.mist_network_client.JoinNetwork(self.mist_local_address, self.mist_network_member_uid)
        if uid:
            self.mist_network_member_uid = uid
            self._RewriteMistIndexFile()
        else:
            self.mist_local_server.Stop()

    def LeaveNetwork(self, local=False):
        if not local:
            self.mist_network_client.LeaveNetwork(self.mist_network_member_uid)
        self.mist_local_server.Stop()
        self.mist_local_server = None
        self.mist_local_address = None
        self.mist_network_client = None
        self.mist_network_address = None

    def StoreDataFile(self, data):
        data_file = data_files.MistDataFile(data, self.mist_network_address, self.root_path)
        self._SetDictKeyValueWithLock(self.mist_data_files, data_file.uid, data_file)
        self._RewriteMistIndexFile()
        return data_file.uid

    def RetrieveDataFile(self, data_uid):
        if data_uid in self.mist_data_files:
            data = self.mist_data_files[data_uid].Read()
            if data is None:
                logger.error("Error in getting data file uid: %s", data_uid)
                return None
            else:
                return data
        else:
            logger.error("Error, invalid data file uid: %s", data_uid)
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
                m.RefreshIndex()
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
