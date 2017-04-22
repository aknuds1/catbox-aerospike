'use strict';

// Load Modules
const Code = require('code');
const Lab = require('lab');
const Catbox = require('catbox');
const Aerospike = require('..');
const Log = require('aerospike').log;

// Internals
const internals = {};

// Test shortcuts

const lab = exports.lab = Lab.script();
const expect = Code.expect;
const describe = lab.describe;
const it = lab.test;

describe('Client', () => {

    it('throws an error if not created with new', (done) => {

        const fn = () => {

            Aerospike();
        };

        expect(fn).to.throw(Error);
        done();
    });

    it('creates a new connection', (done) => {

        const client = new Catbox.Client(Aerospike);
        client.start((err) => {

            expect(err).to.not.exist();
            expect(client.isReady()).to.equal(true);
            done();
        });
    });

    it('closes the connection', (done) => {

        const client = new Catbox.Client(Aerospike);
        client.start((err) => {

            expect(err).to.not.exist();
            expect(client.isReady()).to.equal(true);
            client.stop();
            expect(client.isReady()).to.equal(false);
            done();
        });
    });

    it('gets an item after setting it', (done) => {

        const client = new Catbox.Client(Aerospike);
        client.start((err) => {

            expect(err).to.not.exist();
            const key = { namespace: 'test', id: 'x', segment: 'test' };
            client.set(key, '123', 500, (err) => {

                expect(err).to.not.exist();
                client.get(key, (err, result) => {

                    expect(err).to.not.exist();
                    expect(result.item).to.equal('123');
                    done();
                });
            });
        });
    });

    it('fails setting an item with circular references', (done) => {

        const client = new Catbox.Client(Aerospike);
        client.start((err) => {

            expect(err).to.not.exist();
            const key = { namespace: 'test', id: 'x', segment: 'test' };
            const value = { a: 1 };
            value.b = value;
            client.set(key, value, 10, (err) => {

                expect(err.message).to.equal('Converting circular structure to JSON');
                done();
            });
        });
    });

    it('ignored starting a connection twice on same event', (done) => {

        const client = new Catbox.Client(Aerospike);
        let x = 2;
        const start = () => {

            client.start((err) => {

                expect(err).to.not.exist();
                expect(client.isReady()).to.equal(true);
                --x;
                if (!x){
                    done();
                }
            });

        };
        start();
        start();
    });

    it('ignored starting a connection twice chained', (done) => {

        const client = new Catbox.Client(Aerospike);
        client.start((err) => {

            expect(err).to.not.exist();
            expect(client.isReady()).to.equal(true);

            client.start((err) => {

                expect(err).to.not.exist();
                expect(client.isReady()).to.equal(true);
                done();
            });
        });
    });

    it('returns not found on get when using null key', (done) => {

        const client = new Catbox.Client(Aerospike);
        client.start((err) => {

            expect(err).to.not.exist();
            client.get(null, (err, result) => {

                expect(err).to.equal(null);
                expect(result).to.equal(null);
                done();
            });
        });
    });

    it('returns not found on get when item expired', (done) => {

        const client = new Catbox.Client(Aerospike);
        client.start((err) => {

            expect(err).to.not.exist();
            const key = { id: 'x', namespace: 'test', segment: 'test' };
            client.set(key, 'x', 1, (err) => {

                expect(err).to.not.exist();
                setTimeout(() => {

                    client.get(key, (err, result) => {

                        expect(err).to.equal(null);
                        expect(result).to.equal(null);
                        done();
                    });
                }, 2);
            });
        });
    });

    it('returns error on set when using null key', (done) => {

        const client = new Catbox.Client(Aerospike);
        client.start((err) => {

            expect(err).to.not.exist();
            client.set(null, {}, 1000, (err) => {

                expect(err instanceof Error).to.equal(true);
                done();
            });
        });
    });

    it('returns error on get when using invalid key', (done) => {

        const client = new Catbox.Client(Aerospike);
        client.start((err) => {

            expect(err).to.not.exist();
            client.get({}, (err) => {

                expect(err instanceof Error).to.equal(true);
                done();
            });
        });
    });

    it('returns error on drop when using invalid key', (done) => {

        const client = new Catbox.Client(Aerospike);
        client.start((err) => {

            expect(err).to.not.exist();
            client.drop({}, (err) => {

                expect(err instanceof Error).to.equal(true);
                done();
            });
        });
    });

    it('returns error on set when using invalid key', (done) => {

        const client = new Catbox.Client(Aerospike);
        client.start((err) => {

            expect(err).to.not.exist();
            client.set({}, {}, 1000, (err) => {

                expect(err instanceof Error).to.equal(true);
                done();
            });
        });
    });


    it('ignores set when using non-positive ttl value', (done) => {

        const client = new Catbox.Client(Aerospike);
        client.start((err) => {

            expect(err).to.not.exist();
            const key = { namespace: 'test', id: 'x', segment: 'test' };
            client.set(key, 'y', 0, (err) => {

                expect(err).to.not.exist();
                done();
            });
        });
    });

    it('returns error on drop when using null key', (done) => {

        const client = new Catbox.Client(Aerospike);
        client.start((err) => {

            expect(err).to.not.exist();
            client.drop(null, (err) => {

                expect(err instanceof Error).to.equal(true);
                done();
            });
        });
    });

    it('returns error on get when stopped', (done) => {

        const client = new Catbox.Client(Aerospike);
        client.stop();
        const key = { namespace: 'test', id: 'x', segment: 'test' };
        client.connection.get(key, (err, result) => {

            expect(err).to.exist();
            expect(result).to.not.exist();
            done();
        });
    });

    it('returns error on non string / object keys', (done) => {

        const client = new Catbox.Client(Aerospike);
        const key = [{ namespace: 'test', segment: 'test', id: 'asdf' }];
        const result = client.connection.generateKey(key);
        expect(result).to.be.instanceOf(Error);
        done();
    });

    it('returns error on set when stopped', (done) => {

        const client = new Catbox.Client(Aerospike);
        client.stop();
        const key = { namespace: 'test',  id: 'x', segment: 'test' };
        client.connection.set(key, 'y', 1, (err) => {

            expect(err).to.exist();
            done();
        });
    });

    it('returns error on drop when stopped', (done) => {

        const client = new Catbox.Client(Aerospike);
        client.stop();
        const key = { namespace: 'test', id: 'x', segment: 'test' };
        client.connection.drop(key, (err) => {

            expect(err).to.exist();
            done();
        });
    });

    it('returns error on missing segment name', (done) => {

        const config = {
            expiresIn: 50000
        };
        const fn = () => {

            const client = new Catbox.Client(Aerospike);
            new Catbox.Policy(config, client, '');
        };
        expect(fn).to.throw(Error);
        done();
    });

    it('returns error on bad segment name', (done) => {

        const config = {
            expiresIn: 50000
        };
        const fn = () => {

            const client = new Catbox.Client(Aerospike);
            new Catbox.Policy(config, client, 'a\0b');
        };
        expect(fn).to.throw(Error);
        done();
    });

    it('returns error when cache item dropped while stopped', (done) => {

        const client = new Catbox.Client(Aerospike);
        client.stop();
        client.drop('a', (err) => {

            expect(err).to.exist();
            done();
        });
    });

    describe('#start', () => {

        it('sets client to when the connection succeeds', (done) => {

            const options = {
                hosts: [{
                    addr: '127.0.0.1',
                    port: 3000
                }]
            };

            const aerospike = new Aerospike(options);

            aerospike.start((err) => {

                expect(err).to.not.exist();
                expect(aerospike.client).to.exist();
                done();
            });
        });

        it('reuses the client when a connection is already started', (done) => {

            const options = {
                hosts: [{
                    addr: '127.0.0.1',
                    port: 3000
                }]
            };

            const aerospike = new Aerospike(options);

            aerospike.start((err) => {

                expect(err).to.not.exist();
                const client = aerospike.client;

                aerospike.start((err) => {

                    expect(err).to.not.exist();
                    expect(client).to.equal(aerospike.client);
                    done();
                });
            });
        });

        it('returns an error when connection fails', (done) => {

            const options = {
                hosts: [{
                    addr: '127.0.0.1',
                    port: 3005
                }],
                log: {
                    level: Log.OFF
                }
            };

            const aerospike = new Aerospike(options);

            aerospike.start((err) => {

                expect(err).to.exist();
                expect(err).to.be.instanceOf(Error);
                expect(aerospike.client).to.not.exist();
                done();
            });
        });

    });

    describe('#isReady', () => {

        it('returns true when when connected', (done) => {

            const options = {
                hosts: [{
                    addr: '127.0.0.1',
                    port: 3000
                }]
            };
            const aerospike = new Aerospike(options);

            aerospike.start((err) => {

                expect(err).to.not.exist();
                expect(aerospike.isReady()).to.equal(true);

                aerospike.stop();

                done();
            });
        });

        it('returns false when stopped', (done) => {

            const options = {
                hosts: [{
                    addr: '127.0.0.1',
                    port: 3000
                }]
            };
            const aerospike = new Aerospike(options);

            aerospike.start((err) => {

                expect(err).to.not.exist();
                expect(aerospike.isReady()).to.equal(true);

                aerospike.stop();

                expect(aerospike.isReady()).to.equal(false);

                done();
            });
        });

        // Aerospike client does not trigger on close event
        // Hard to detect and update isReady status
        // Need to figure out a solution
        /*
        it ('returns false when disconnected', (done) => {

            const options = {
                hosts: [{
                    addr: '127.0.0.1',
                    port: 3000
                }]
            };
            const aerospike = new Aerospike(options);

            aerospike.start((err) => {

                expect(err).to.not.exist();
                expect(aerospike.client).to.exist();
                expect(aerospike.isReady()).to.equal(true);

                aerospike.client.close();

                expect(aerospike.isReady()).to.equal(false);

                aerospike.stop();
                done();
            });
        });
        */
    });
    describe('#validateSegmentName', () => {

        it('returns an error when the name is empty', (done) => {

            const options = {
                hosts: [{
                    addr: '127.0.0.1',
                    port: 3000
                }]
            };

            const aerospike = new Aerospike(options);

            const result = aerospike.validateSegmentName('');

            expect(result).to.be.instanceOf(Error);
            expect(result.message).to.equal('Empty string');
            done();
        });

        it('returns an error when the name has a null character', (done) => {

            const options = {
                hosts: [{
                    addr: '127.0.0.1',
                    port: 3000
                }]
            };

            const aerospike = new Aerospike(options);

            const result = aerospike.validateSegmentName('\0test');

            expect(result).to.be.instanceOf(Error);
            done();
        });

        it('returns null when there aren\'t any errors', (done) => {

            const options = {
                hosts: [{
                    addr: '127.0.0.1',
                    port: 3000
                }]
            };

            const aerospike = new Aerospike(options);

            const result = aerospike.validateSegmentName('valid');

            expect(result).to.not.be.instanceOf(Error);
            expect(result).to.equal(null);
            done();
        });
    });

    describe('#get', () => {

        it('passes an error to the callback when the connection is closed', (done) => {

            const options = {
                hosts: [{
                    addr: '127.0.0.1',
                    port: 3000
                }]
            };

            const aerospike = new Aerospike(options);

            aerospike.get('test', (err) => {

                expect(err).to.exist();
                expect(err).to.be.instanceOf(Error);
                expect(err.message).to.equal('Connection not started');
                done();
            });
        });

        it('passes an error to the callback when there is an error returned from getting an item', (done) => {

            const options = {
                hosts: [{
                    addr: '127.0.0.1',
                    port: 3000
                }]
            };

            const aerospike = new Aerospike(options);
            aerospike.client = {
                get: function (item, callback) {

                    callback({ code: 'ERR' });
                }
            };

            aerospike.get('test', (err) => {

                expect(err).to.exist();
                expect(err).to.be.instanceOf(Error);
                done();
            });
        });

        it('passes an error to the callback when there is an error parsing the result', (done) => {

            const options = {
                hosts: [{
                    addr: '127.0.0.1',
                    port: 3000
                }]
            };

            const aerospike = new Aerospike(options);
            aerospike.client = {
                get: function (item, callback) {

                    callback(null, 'test');
                }
            };

            aerospike.get('test', (err) => {

                expect(err).to.exist();
                expect(err.message).to.equal('Bad envelop content');
                done();
            });
        });

        it('passes an error to the callback when there is an error with the envelope structure (stored)', (done) => {

            const options = {
                hosts: [{
                    addr: '127.0.0.1',
                    port: 3000
                }]
            };

            const aerospike = new Aerospike(options);
            aerospike.client = {
                get: function (item, callback) {

                    callback(null, '{ "item": "false" }');
                }
            };

            aerospike.get('test', (err) => {

                expect(err).to.exist();
                expect(err.message).to.equal('Bad envelop content');
                done();
            });
        });

        it('passes an error to the callback when there is an error with the envelope structure (item)', (done) => {

            const options = {
                hosts: [{
                    addr: '127.0.0.1',
                    port: 3000
                }]
            };

            const aerospike = new Aerospike(options);
            aerospike.client = {
                get: function (item, callback) {

                    callback(null, '{ "stored": "123" }');
                }
            };

            aerospike.get('test', (err) => {

                expect(err).to.exist();
                expect(err.message).to.equal('Bad envelop content');
                done();
            });
        });

        it('is able to retrieve an object thats stored when connection is started', (done) => {

            const options = {
                hosts: [{
                    addr: '127.0.0.1',
                    port: 3000
                }]
            };
            const key = {
                id: 'test',
                segment: 'test'
            };

            const aerospike = new Aerospike(options);

            aerospike.start(() => {

                aerospike.set(key, 'myvalue', 200, (err) => {

                    expect(err).to.not.exist();
                    aerospike.get(key, (err, result) => {

                        expect(err).to.not.exist();
                        expect(result.item).to.equal('myvalue');
                        done();
                    });
                });
            });
        });

        it('returns null when unable to find the item', (done) => {

            const options = {
                hosts: [{
                    addr: '127.0.0.1',
                    port: 3000
                }],
                partition: 'test',
                segment: 'test'
            };
            const key = {
                id: 'notfound'
            };

            const aerospike = new Aerospike(options);

            aerospike.start(() => {

                aerospike.get(key, (err, result) => {

                    expect(err).to.not.exist();
                    expect(result).to.not.exist();
                    done();
                });
            });
        });
    });

    describe('#set', () => {

        it('passes an error to the callback when the connection is closed', (done) => {

            const options = {
                hosts: [{
                    addr: '127.0.0.1',
                    port: 3000
                }]
            };

            const aerospike = new Aerospike(options);

            aerospike.set('test1', 'test1', 3600, (err) => {

                expect(err).to.exist();
                expect(err).to.be.instanceOf(Error);
                expect(err.message).to.equal('Connection not started');
                done();
            });
        });

        it('passes an error to the callback when there is an error returned from setting an item', (done) => {

            const options = {
                hosts: [{
                    addr: '127.0.0.1',
                    port: 3000
                }]
            };

            const aerospike = new Aerospike(options);
            aerospike.client = {
                put: function (key, item, meta, policy, callback) {

                    callback(new Error('error'));
                }
            };

            aerospike.set('test', 'test', 3600, (err) => {

                expect(err).to.exist();
                expect(err).to.be.instanceOf(Error);
                done();
            });
        });
    });

    describe('#drop', () => {

        it('passes an error to the callback when the connection is closed', (done) => {

            const options = {
                hosts: [{
                    addr: '127.0.0.1',
                    port: 3000
                }]
            };

            const aerospike = new Aerospike(options);

            aerospike.drop('test2', (err) => {

                expect(err).to.exist();
                expect(err).to.be.instanceOf(Error);
                expect(err.message).to.equal('Connection not started');
                done();
            });
        });

        it('passes an error to the callback when there is an error returned from dropping an item', (done) => {

            const options = {
                hosts: [{
                    addr: '127.0.0.1',
                    port: 3000
                }]
            };

            const aerospike = new Aerospike(options);
            aerospike.client = {
                remove: function (key, callback) {

                    callback({ code: 'err' }, null);
                }
            };

            aerospike.drop('test', (err) => {

                expect(err).to.exist();
                expect(err).to.be.instanceOf(Error);
                done();
            });
        });

        it('deletes the item from aerospike', (done) => {

            const options = {
                hosts: [{
                    addr: '127.0.0.1',
                    port: 3000
                }]
            };

            const aerospike = new Aerospike(options);
            aerospike.client = {
                remove: function (key, callback) {

                    callback(null, null);
                }
            };

            aerospike.drop('test', (err) => {

                expect(err).to.not.exist();
                done();
            });
        });
    });

    describe('#stop', () => {

        it('sets the client to null', (done) => {

            const options = {
                hosts: [{
                    addr: '127.0.0.1',
                    port: 3000
                }]
            };

            const aerospike = new Aerospike(options);

            aerospike.start(() => {

                expect(aerospike.client).to.exist();
                aerospike.stop();
                expect(aerospike.client).to.not.exist();
                done();
            });
        });
    });
});
