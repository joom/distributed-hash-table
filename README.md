# rpc

A remote procedure call implementation with locks and a view leader.

## Installation

Make sure you have [Stack](http://haskellstack.org) installed.

Clone the repository and run `stack install`. The executable files
`rpc-client`, `rpc-server`, `rpc-view-leader` must now be `~/.local/bin`, which
is probably in your path variable.

## Usage

You can run the server without any command line arguments: `rpc-server`.

Running the client is a bit more complicated, there are 7 kinds of procedures
you can call. Here's an example of each (and more):

```
~$ rpc-client print "side effects yay!"
Connected to localhost:38000
{"status":"ok","tag":"Executed","i":1}

~$ rpc-client set "lang" "русски"
Connected to localhost:38000
{"status":"ok","tag":"Executed","i":1}

~$ rpc-client get "lang"
Connected to localhost:38000
{"status":"ok","tag":"GetResponse","value":"русски","i":1}

~$ rpc-client get "timezone"
Connected to localhost:38000
{"status":"not_found","tag":"GetResponse","value":"","i":1}

~$ rpc-client query_all_keys
Connected to localhost:38000
{"status":"ok","tag":"KeysResponse","keys":["lang"],"i":1}

~$ rpc-client query_servers
Connected to localhost:39000
{"tag":"QueryServersResponse","result":["127.0.0.1:54945"],"epoch":1,"i":1}

~$ rpc-client lock_get "jane" "user1"
Connected to localhost:39000
{"status":"granted","tag":"Executed","i":1}

~$ rpc-client lock_get "jack" "user2"
Connected to localhost:39000
{"status":"granted","tag":"Executed","i":1}

~$ rpc-client lock_release "jane" "user2"
Connected to localhost:39000
{"status":"forbidden","tag":"Executed","i":1}

~$ rpc-client lock_release "jane" "user1"
Connected to localhost:39000
{"status":"ok","tag":"Executed","i":1}

~$ rpc-client lock_get "jack" "user1"
Connected to localhost:39000
{"status":"retry","tag":"Executed","i":1}
Waiting for 5 sec
Connected to localhost:39000
{"status":"retry","tag":"Executed","i":2}
Waiting for 5 sec
...
```

You can ignore the `tag` field in the response, but it is kept for now since it
makes the `FromJSON Response` instance easier.

The executable  `rpc-client` take optional arguments `--server` (or `-s`) and
`--viewleader` (or `-l`) that specifies which host to connect , such a call
would be of the form

```
rpc-client --server 127.0.0.1 set "lang" "Türkçe"
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
