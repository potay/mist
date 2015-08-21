import uuid
import random
import pickle
import os
import pyjsonrpc
import threading
import time
from gevent import pool
import mist_network_member


class MistNetworkError(Exception):
    pass


class MistNetworkClient(pyjsonrpc.HttpClient):
    def JoinNetwork(self, member_address, member_uid=None):
        try:
            if self.mist_network_member_uid:
                response = self.join(member_address, str(member_uid))
            else:
                response = self.join(self.mist_local_address)
            return uuid.UUID(response["network_member_uid"])
        except:
            print "Could not join network. Network address: %s" % self.url
            return None

    def LeaveNetwork(self, member_uid):
        self.mist_network_client.leave(str(member_uid))

    def StoreDataOnNetwork(self, data):
        encoding = "base64"
        data_base64 = data.encode(encoding, "strict")
        response = self.store(data_base64, encoding)
        mist_network_member_uid = response["network_member_uid"]
        data_uid = uuid.UUID(response["data_uid"])
        return (mist_network_member_uid, data_uid)

    def RetrieveDataOnNetwork(self, member_uid, data_uid):
        response = self.retrieve(member_uid, str(data_uid))
        if "error_message" in response:
            print "Unable to read data file. Error: %s" % response["error_message"]
            return None
        else:
            return response["data"].decode(response["encoding"])

    def DeleteDataOnNetwork(self, member_uid, data_uid):
        response = self.delete(member_uid, str(data_uid))
        if response and "error_message" in response:
            print response["error_message"]
            return False
        return True


class MistNetworkServerHTTPRequestHandler(pyjsonrpc.HttpRequestHandler):
    @pyjsonrpc.rpcmethod
    def join(self, member_address, member_uid=None):
        if member_uid:
            member_uid = self.server.AddMember(member_address, uuid.UUID(member_uid))
        else:
            member_uid = self.server.AddMember(member_address)
        return {"network_member_uid": str(member_uid)}

    @pyjsonrpc.rpcmethod
    def leave(self, member_uid):
        self.server.DeleteMember(uuid.UUID(member_uid))
        return True

    @pyjsonrpc.rpcmethod
    def store(self, data, encoding="ascii"):
        (member_uid, data_uid) = self.server.ProcessStoreRequest(data.decode(encoding))
        return {"network_member_uid": str(member_uid), "data_uid": str(data_uid)}

    @pyjsonrpc.rpcmethod
    def retrieve(self, member_uid, data_uid):
        try:
            data = self.server.ProcessRetrieveRequest(uuid.UUID(member_uid), uuid.UUID(data_uid))
            if data:
                return {"data": data.encode("base64"), "encoding": "base64"}
            else:
                return {"error_message": "Unable to retrieve data."}
        except MistNetworkError as e:
            return {"error_message": str(e)}

    @pyjsonrpc.rpcmethod
    def delete(self, member_uid, data_uid):
        try:
            self.server.ProcessDeleteRequest(uuid.UUID(member_uid), uuid.UUID(data_uid))
        except MistNetworkError as e:
            return {"error_message": str(e)}


class MistNetworkServer(pyjsonrpc.ThreadingHttpServer):
    """Mist Network Class"""

    DEFAULT_NETWORK_STATE_FILENAME = "network_state"
    DEFAULT_SERVER_HANDLER = MistNetworkServerHTTPRequestHandler
    SCHEDULE_REQUEST_DELAY = 5 * 60
    DEFAULTGREENLET_POOL_SIZE = 100

    def __init__(self, name, server_address):
        pyjsonrpc.ThreadingHttpServer.__init__(self, server_address=server_address, RequestHandlerClass=MistNetworkServer.DEFAULT_SERVER_HANDLER)
        self.name = name
        self.network_members = {}
        self.inactive_network_members = {}
        self.greenlet_pool = pool.Pool(size=MistNetworkServer.DEFAULTGREENLET_POOL_SIZE)

    def _LoadMistNetworkState(self):
        if os.path.isfile("%s_%s" % (self.name, MistNetworkServer.DEFAULT_NETWORK_STATE_FILENAME)):
            with open("%s_%s" % (self.name, MistNetworkServer.DEFAULT_NETWORK_STATE_FILENAME), "r") as infile:
                (self.network_members, self.inactive_network_members) = pickle.load(infile)
        else:
            self._RewriteNetworkStateFile()

    def _RewriteNetworkStateFile(self):
        with open("%s_%s" % (self.name, MistNetworkServer.DEFAULT_NETWORK_STATE_FILENAME), "wb") as outfile:
            pickle.dump((self.network_members, self.inactive_network_members), outfile)

    def Start(self):
        self._LoadMistNetworkState()
        threading.Thread(target=self.serve_forever).start()

    def Stop(self):
        self.DisconnectAllMembers()
        self.shutdown()
        self._RewriteNetworkStateFile()

    def DisconnectAllMembers(self):
        print "Disconnecting all members. Count: %s" % len(self.network_members)
        keys = self.network_members.keys()
        for member_uid in keys:
            self.network_members[member_uid].SendDisconnectRequest()
            self.DeleteMember(member_uid)

    def AddMember(self, member_address, member_uid=None):
        if member_uid and member_uid in self.inactive_network_members:
            member = self.inactive_network_members[member_uid]
            member.Activate(member_address)
            self.network_members[member_uid] = member
            del self.inactive_network_members[member_uid]
        else:
            member = mist_network_member.MistNetworkMember(member_address)
            self.network_members[member.uid] = member
        self._RewriteNetworkStateFile()
        print "%s just joined." % str(member)
        return member.uid

    def DeleteMember(self, member_uid):
        if member_uid in self.network_members:
            print "%s just left." % str(self.network_members[member_uid])
            self.inactive_network_members[member_uid] = self.network_members[member_uid]
            self.inactive_network_members[member_uid].Deactivate()
            del self.network_members[member_uid]
            self._RewriteNetworkStateFile()

    def GetRandomMember(self):
        return random.choice(self.network_members.values())

    def ListMembers(self):
        return map(str, self.network_members.values())

    def ProcessStoreRequest(self, data):
        chosen_member = self.GetRandomMember()
        # greenlet = self.greenlet_pool.spawn(chosen_member.SendStoreRequest, data)
        # greenlet.join()
        # data_uid = greenlet.value
        data_uid = chosen_member.SendStoreRequest(data)
        return (chosen_member.uid, data_uid)

    def ProcessRetrieveRequest(self, member_uid, data_uid):
        if member_uid in self.network_members:
            # greenlet = self.greenlet_pool.spawn(self.network_members[member_uid].SendRetrieveRequest, data_uid)
            # greenlet.join()
            # data = greenlet.value
            data = self.network_members[member_uid].SendRetrieveRequest(data_uid)
            return data
        else:
            raise MistNetworkError("Network member is not connected. Member uid: %s" % member_uid)

    def ProcessDeleteRequest(self, member_uid, data_uid):
        if member_uid in self.network_members:
            # greenlet = self.greenlet_pool.spawn(self.network_members[member_uid].SendDeleteRequest, data_uid)
            # greenlet.join()
            # if not greenlet.value:
            if not self.network_members[member_uid].SendDeleteRequest(data_uid):
                self.ScheduleRequest(self.ProcessDeleteRequest, member_uid, data_uid)
        else:
            raise MistNetworkError("Network member is not connected. Member uid: %s" % member_uid)

    def ScheduleRequest(self, process_request_func, *args, **kwargs):
        threading.Thread(target=self._ScheduledRequestWorker, args=(process_request_func,)+args, kwargs=kwargs)

    def _ScheduledRequestWorker(self, process_request_func, *args, **kwargs):
        time.sleep(MistNetworkServer.SCHEDULE_REQUEST_DELAY)
        process_request_func(*args, **kwargs)


def main():
    network = MistNetworkServer("potay", ("localhost", 15112))
    network.Start()
    print "network:", network.ListMembers()
    try:
        try:
            while True:
                print
                raw_input("Boo!")
                print "network:", network.ListMembers()
                print "Active Members:"
                for member in network.network_members.values():
                    print "  %s: %s" % (str(member), member._history[-5:])
                print "Inactive Members:"
                for member in network.inactive_network_members.values():
                    print "  %s: %s" % (str(member), member._history[-5:])
                else:
                    print "  None"
        except KeyboardInterrupt:
            print "Quitting..."
    finally:
        print "network:", network.ListMembers()
        network.Stop()


if __name__ == "__main__":
    main()
