import uuid
import random
import pickle
import os
import pyjsonrpc
import threading


class MistNetworkError(Exception):
    pass


class MistNetworkMember(object):
    """Mist Network Member Class"""

    def __init__(self, mist_address):
        self.uid = uuid.uuid4()
        self.mist_address = None
        self._history = []
        self.active = False
        self.Activate(mist_address)

    def Activate(self, mist_address):
        self.mist_address = mist_address
        self.active = True
        self._history.append("Activated: %s" % mist_address)

    def Deactivate(self):
        self.mist_address = None
        self.active = False
        self._history.append("Deactivated")

    def SendStoreRequest(self, data):
        self._history.append("store: size: %s" % len(data))
        mist_member_client = MistNetworkClient(self.mist_address)
        response = mist_member_client.store(data)
        return uuid.UUID(response["data_uid"])

    def SendRetrieveRequest(self, data_uid):
        self._history.append("retrieve: %s" % data_uid)
        mist_member_client = MistNetworkClient(self.mist_address)
        response = mist_member_client.retrieve(str(data_uid))
        return response["data"]

    def SendDeleteRequest(self, data_uid):
        self._history.append("delete: %s" % data_uid)
        mist_member_client = MistNetworkClient(self.mist_address)
        mist_member_client.delete(str(data_uid))

    def __str__(self):
        if self.active:
            return "%s@%s" % (self.uid, self.mist_address)
        else:
            return "%s" % self.uid


class MistNetworkClient(pyjsonrpc.HttpClient):
    pass


class MistNetworkMemberServerHTTPRequestHandler(pyjsonrpc.HttpRequestHandler):
    @pyjsonrpc.rpcmethod
    def store(self, data):
        data_uid = self.server.StoreData(data)
        return {"data_uid": str(data_uid)}

    @pyjsonrpc.rpcmethod
    def retrieve(self, data_uid):
        data = self.server.RetrieveData(uuid.UUID(data_uid))
        return {"data": data}

    @pyjsonrpc.rpcmethod
    def delete(self, data_uid):
        self.server.DeleteData(uuid.UUID(data_uid))


class MistNetworkMemberServer(pyjsonrpc.ThreadingHttpServer):
    DEFAULT_SERVER_HANDLER = MistNetworkMemberServerHTTPRequestHandler

    def __init__(self, mist, host):
        pyjsonrpc.ThreadingHttpServer.__init__(self, server_address=(host, 0), RequestHandlerClass=MistNetworkMemberServer.DEFAULT_SERVER_HANDLER)
        self.server_address = self.socket.getsockname()
        self.mist = mist

    def Start(self):
        threading.Thread(target=self.serve_forever).start()

    def Stop(self):
        self.shutdown()

    def StoreData(self, data):
        return self.mist.StoreDataFile(data)

    def RetrieveData(self, data_uid):
        return self.mist.RetrieveDataFile(data_uid)

    def DeleteData(self, data_uid):
        self.mist.DeleteDataFile(data_uid)


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
    def store(self, data):
        (member_uid, data_uid) = self.server.ProcessStoreRequest(data)
        return {"network_member_uid": str(member_uid), "data_uid": str(data_uid)}

    @pyjsonrpc.rpcmethod
    def retrieve(self, member_uid, data_uid):
        try:
            data = self.server.ProcessRetrieveRequest(uuid.UUID(member_uid), uuid.UUID(data_uid))
            return {"data": data}
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
    ENCRYPTION_KEY = "ChangeThisPlease"
    DEFAULT_SERVER_HANDLER = MistNetworkServerHTTPRequestHandler

    def __init__(self, name, server_address):
        pyjsonrpc.ThreadingHttpServer.__init__(self, server_address=server_address, RequestHandlerClass=MistNetworkServer.DEFAULT_SERVER_HANDLER)
        self.name = name
        self.network_members = {}
        self.inactive_network_members = {}

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
        # self.httpd.socket = ssl.wrap_socket (self.httpd.socket, certfile='path/to/localhost.pem', server_side=True)
        threading.Thread(target=self.serve_forever).start()

    def Stop(self):
        self.shutdown()
        self._RewriteNetworkStateFile()

    def AddMember(self, member_address, member_uid=None):
        if member_uid and member_uid in self.inactive_network_members:
            member = self.inactive_network_members[member_uid]
            member.Activate(member_address)
            self.network_members[member_uid] = member
            del self.inactive_network_members[member_uid]
        else:
            member = MistNetworkMember(member_address)
            self.network_members[member.uid] = member
        self._RewriteNetworkStateFile()
        return member.uid

    def DeleteMember(self, member_uid):
        if member_uid in self.network_members:
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
        data_uid = chosen_member.SendStoreRequest(data)
        return (chosen_member.uid, data_uid)

    def ProcessRetrieveRequest(self, member_uid, data_uid):
        if member_uid in self.network_members:
            return self.network_members[member_uid].SendRetrieveRequest(data_uid)
        else:
            raise MistNetworkError("Network member is not connected. Member uid: %s" % member_uid)

    def ProcessDeleteRequest(self, member_uid, data_uid):
        if member_uid in self.network_members:
            return self.network_members[member_uid].SendDeleteRequest(data_uid)
        else:
            raise MistNetworkError("Network member is not connected. Member uid: %s" % member_uid)


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
                    print "  %s: %s" % (str(member), member._history)
                print "Inactive Members:"
                for member in network.inactive_network_members.values():
                    print "  %s: %s" % (str(member), member._history)
                else:
                    print "  None"
        except KeyboardInterrupt:
            print "network:", network.ListMembers()
            network.Stop()
    finally:
        print "network:", network.ListMembers()
        network.Stop()


if __name__ == "__main__":
    main()
