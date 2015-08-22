# mist

Mist is an open source decentralized cloud storage network system.

To start the network, run:
```
python network.py
```

Before creating your account, you should change your encryption password. To change the encryption password, edit the following line in ```settings.py```:
```shell
PASSWORD = "ChangeThisPlease"
```

To connect to the network, run:
```shell
python mist.py "Your account name"
```

Upon connecting to the network, a folder in ```accounts/``` will be created
and any files in that folder will be synced on the network automatically.
