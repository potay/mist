import uuid
import pyjsonrpc
import threading
import mist_network


class MistNetworkMemberError(Exception):
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
        mist_member_client = mist_network.MistNetworkClient(self.mist_address)
        response = mist_member_client.store(data.encode("base64"), encoding="base64")
        return uuid.UUID(response["data_uid"])

    def SendRetrieveRequest(self, data_uid):
        self._history.append("retrieve: %s" % data_uid)
        mist_member_client = mist_network.MistNetworkClient(self.mist_address)
        response = mist_member_client.retrieve(str(data_uid))
        if "error_message" in response:
            print response["error_message"]
            return None
        else:
            return response["data"].decode(response["encoding"])

    def SendDeleteRequest(self, data_uid):
        self._history.append("delete: %s" % data_uid)
        mist_member_client = mist_network.MistNetworkClient(self.mist_address)
        return mist_member_client.delete(str(data_uid))

    def SendDisconnectRequest(self):
        self._history.append("disconnect request")
        mist_member_client = mist_network.MistNetworkClient(self.mist_address)
        mist_member_client.disconnect()

    def __str__(self):
        if self.active:
            return "%s@%s" % (self.uid, self.mist_address)
        else:
            return "%s" % self.uid


class MistNetworkMemberServerHTTPRequestHandler(pyjsonrpc.HttpRequestHandler):
    @pyjsonrpc.rpcmethod
    def store(self, data, encoding="ascii"):
        data_uid = self.server.StoreData(data.decode(encoding))
        return {"data_uid": str(data_uid)}

    @pyjsonrpc.rpcmethod
    def retrieve(self, data_uid):
        data = self.server.RetrieveData(uuid.UUID(data_uid))
        if data is None:
            return {"error_message": "Unable to retrieve data."}
        else:
            return {"data": data.encode("base64"), "encoding": "base64"}

    @pyjsonrpc.rpcmethod
    def delete(self, data_uid):
        return self.server.DeleteData(uuid.UUID(data_uid))

    @pyjsonrpc.rpcmethod
    def disconnect(self):
        return self.server.LeaveNetwork()


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
        return self.mist.DeleteDataFile(data_uid)

    def LeaveNetwork(self):
        self.mist.LeaveNetwork(local=True)
