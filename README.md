# catbox-aerospike
Caching Adapter for [Aerospike](http://www.aerospike.com) for [catbox](https://github.com/hapijs/catbox)

## Versions
* `v 2.x.x`: `node v 4.x.x` and `aerospike client v 2.x.x`
* `v 1.x.x`: `node v 4.x.x` and `aerospike client v 1.x.x`
* `v 0.x.x`: `node v 0.10.x` and `aerospike client v 1.x.x`

[![npm version](https://badge.fury.io/js/catbox-aerospike.png)](http://npmjs.org/package/catbox-aerospike)
[![Build Status](https://travis-ci.org/ooogway/catbox-aerospike.svg?branch=master)](https://travis-ci.org/ooogway/catbox-aerospike)

## Installation
```sh
$ npm install catbox-aerospike --save
```

## Options

- `hosts` - Array of Aerospike servers
   - `addr` - Aerospike server hostname. Defaults to `'127.0.0.1'`.
   - `port` - Aerospike server port or unix domain socket path. Defaults to `3000`.
- `partition` - corresponds to [Aerospike namespace](http://www.aerospike.com/docs/architecture/data-model.html#namespaces). Defaults to `test`
- `segment` - corresponds to [Aerospike set](http://www.aerospike.com/docs/architecture/data-model.html#sets). Defaults to `test`

**NOTE** Aerospike Namespaces are configured when the cluster is started, and cannot be created at runtime. Default Aerospike namespace is `test`. However, Catbox intializes adapters with default partition name `catbox`, if no partition name is configured. Please refer to [Namespace Configuration](http://www.aerospike.com/docs/operations/configure/namespace/) to use appropriate namespace.

## Initialization

```javascript
const Hapi = require('hapi');

const server = new Hapi.Server({
    cache: [
        {
            name: 'aeroCache',
            engine: require('catbox-aerospike'),
            partition: 'cache'
            host: [
                {
                    addr: '127.0.0.1',
                    port: 3000
                }
            ]
        }
    ]
});
```
**Using Glue Manifest JSON:**
```javascript
{
    "server": {
        "app": {
            ...
        },
        "connections": {
            ...
        },
        "cache": {
            "name": "aeroCache"
            "engine": "catbox-aerospike"
            "partition": "cache"
            "hosts": [
                {
                    "addr": "127.0.0.1",
                    "port": 3000
                }
            ]
        }
    },
    "connections": {
        ...
    },
    "plugins": {
        ...
    }
}
```

## Usage

```javascript
const add = function (a, b, next) {

    return next(null, Number(a) + Number(b));
};

server.method('sum', add, {
    cache: {
        cache: 'aeroCache',
        expiresIn: 30 * 1000,
        generateTimeout: 100
    }
});

server.route({
    path: '/add/{a}/{b}',
    method: 'GET',
    handler: function (request, reply) {

        server.methods.sum(request.params.a, request.params.b, function (err, result) {

            reply(result);
        });
    }
});
```

## Tests

The test suite expects an Aerospike server to be running on port 3000. Refer to Aerospike [Installation guide](http://www.aerospike.com/docs/operations/install/) OR run Aerospike server in a Docker container

```sh
// Running Aerospike Server
$ docker run -it -d -p 3000:3000 aerospike/aerospike-server

// Running Tests
$ npm test
```
