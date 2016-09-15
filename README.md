# rpc

A simple remote procedure call implementation.

## Installation

Make sure you have [Stack](http://haskellstack.org) installed.

Clone the repository and run `stack install`. The executable files `rpc-client` and `rpc-server` must now be `~/.local/bin`, which is probably in your path variable.

## Usage

You can run the server without any command line arguments: `rpc-server`.

Running the client is a bit more complicated, there are 4 kinds of procedures you can call. Here's an example of each (and more):

```
~$ rpc-client print "side effects yay!"
Connected to localhost:38000
{"status":"ok","tag":"Executed","i":1}

~$ rpc-client set "lang" "русски"
Connected to localhost:38000
{"status":"ok","tag":"Executed","i":2}

~$ rpc-client get "lang"
Connected to localhost:38000
{"status":"ok","tag":"GetResponse","value":"русски","i":3}

~$ rpc-client get "timezone"
Connected to localhost:38000
{"status":"not_found","tag":"GetResponse","value":"","i":4}

~$ rpc-client query_all_keys
Connected to localhost:38000
{"status":"ok","tag":"KeysResponse","keys":["lang"],"i":5}
```

You can ignore the `tag` field in the response, but it is kept for now since it will make the `FromJSON Response` instance easier later.

The executable `rpc-client` also takes an optional argument `--server` (or `-s`) that specifies which host to connect, such a call would be of the form

```
rpc-client --server 127.0.0.1 set "lang" "Türkçe"
```

If the optional argument isn't provided, it is assumed to be "localhost".

If a connection to a host name and a port, or a bind to a port doesn't happen in 10 seconds,
you will get a timeout error and the program will try the next available port number.
