# rpc

An implementation of distributed hash tables with distributed two-phase commit. The view leader can hold locks.

Written as a project for COMP360 Distributed Systems, Fall 2016, Prof. Jeff
Epstein, Wesleyan University.

## Installation

Make sure you have [Stack](http://haskellstack.org) installed.

Clone the repository and run `stack install`. The executable files
`rpc-client`, `rpc-server`, `rpc-view-leader` must now be `~/.local/bin`, which
is probably in your path variable.

## Usage

You can run the server without any command line arguments: `rpc-server`.

For `rpc-client` and `rpc-view-leader`, you can see all the options using the `--help` flag.

The executable  `rpc-client` take optional arguments `--server` (or `-s`) and
`--viewleader` (or `-l`) that specifies which host to connect , such a call
would be of the form

```
rpc-client --server 127.0.0.1 setr "lang" "Türkçe"
```

The executable `rpc-server` also takes the `--viewleader` optional argument, to
be able to perform heartbeats.

If the optional argument isn't provided, it is assumed to be "localhost".

### Timeouts

If a connection to a host name and a port, or a bind to a port doesn't happen
in 10 seconds, you will get a timeout error and the program will try the next
available port number.

If a server fails to send a heartbeat for 30 seconds, but then later succeeds,
it will be told by the view leader that it expired. In that case, the server
terminates.

### Lock cancelling after crash and deadlock detection

The view leader assumes that a server crashed if it sends no heartbeat for 30
seconds.  In that case, if that server holds any locks (that have the requester
identifier format `:38000`, where the number is the port) are cancelled
automatically.

When the server has to return retry for a lock get request, it checks if there
is a deadlock in the system by building a directed graph and looking for
cycles. If there is, it logs the requesters and locks involved. Right now, it
repeatedly logs the same deadlock information as long as it keeps getting the
request. Since the client keeps retrying by default, it will not stop logging
the deadlock unless the client is stopped. If a requester ID is used for
multiple lock requests at the same time, this can cause some weird behavior.

### Bucket allocator

We assume that our hash function evenly distributes strings to integers. We
take the UUID of every server to be the string for its hash, i.e. the hash
value of the server. Therefore we assume that our servers more or less evenly
split the number of keys we want to hold. The primary bucket to hold a key is
the bucket that has the server that has the lowest hash value that still is
strictly greater than the hash of the key string. We hold 2 more replicas as
backup, in servers that have the next 2 lowest hash value.

### Rebalancing

If some servers are removed, then each server will check if the keys they
currently hold used to reside in one of the servers that are removed.  If
that's the case, then it will make a request to the new server that is suppose
to hold the data, to save the data.

When there are new servers, each server will check what keys they have.  If a
server has a key that should reside in a different server now, it will make a
request to that different server and then delete that key from itself.
