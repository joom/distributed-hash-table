# rpc

A simple remote procedure call implementation.

## Installation

Make sure you have [Stack](http://haskellstack.org) installed.

Clone the repository and run `stack install`. The executable files `rpc-client` and `rpc-server` must now be in your path.

## Usage

You can run the server without any command line arguments: `rpc-server`.

Running the client is a bit more complicated, there are 4 kinds of procedures you can call. Here's an example of each:

```
rpc-client print "side effects yay!"

rpc-client set "lang" "русски"

rpc-client get "lang"

rpc-client query_all_keys
```

The executable `rpc-client` also takes an optional argument `--server` (or `-s`) that specifies which host to connect, such a call would be of the form

```
rpc-client --server 127.0.0.1 set "lang" "Türkçe"
```

If the optional argument isn't provided, it is assumed to be "localhost".
