# Mist

## What is Mist?
Mist is an open source decentralized cloud storage network system.

## Getting Started
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

## Issues
* Network faces overloading issues when handling the distribution of chunks
of a large file and results in broken pipe errors. Add failover measures to
handle such errors.

## TODOs
1. Remove network as the middleman for data transfer and use it only as a
peer discovery medium.
1. Reconsider the recursive nature of breaking up the files. Is it necessary
and worth the cost?
1. Add better data integrity checks and handling to ensure reliability.
1. Add redundancy to ensure data availability.
1. Add self healing measures such as data duplication if a data chunk becomes
corrupted or unavailable.
