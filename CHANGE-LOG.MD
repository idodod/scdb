# CHANGE-LOG

## 2023-11-14 scdb v1.0.0-alpha release

- Currently, it is at a very basic stage, with only get, set, delete implemented, and it only supports the golang client and the grpc protocol.

- Currently, I am working to support the TCP protocol, but starting from scratch with TCP is different from my previous work with grpc and general servers, so it might take some time.


## Short-term Goal (Within 1 Month)

- I am looking forward to the official release of version 1.0.0 after working on the TCP protocol and adding transactions.


## Mid to Long-term Goal (More than 3 Months)

- I want to support various programming language SDKs.
- I want to introduce high-availability distributed processing through the RAFT algorithm.
- I currently want to change the BTree structure to allow for faster data access.