'use strict';

// Load Modules
const Aerospike = require('aerospike');
const Hoek = require('hoek');

// Internals
const internals = {};

internals.defaults = {
    hosts: [{
        addr: '127.0.0.1',
        port: 3000
    }],
    segment: 'test',
    partition: 'test'
};

module.exports = class {

    constructor(options) {

        this._settings = Hoek.applyToDefaults(internals.defaults, options);
        this.client = null;
        this._connected = false;
    }

    start(next) {

        if (this.client){
            return Hoek.nextTick(next)();
        }

        Aerospike.connect(this._settings, (error, client) => {

            if (error){
                return next(error);
            }
            this.client = client;
            this._connected = true;
            return next();
        });
    }

    stop() {

        if (this.client) {
            this.client.close();
            this._connected = false;
            this.client = null;
        }
    }

    isReady() {

        return !!this.client && this._connected;
    }

    validateSegmentName(name) {

        if (!name) {
            return new Error('Empty string');
        }

        if (name.indexOf('\0') !== -1) {
            return new Error('Includes null character');
        }

        return null;

    }

    get(key, callback) {

        if (!this.client){
            return callback(new Error('Connection not started'));
        }

        this.client.get(this.generateKey(key), (err, record, meta) => {

            if (err){
                if (err.code === Aerospike.status.AEROSPIKE_ERR_RECORD_NOT_FOUND){
                    return callback(null, null);
                }
                return callback(new Error('Error getting result'), null);
            }
            if (!record.hasOwnProperty('item') || !record.hasOwnProperty('stored')){
                return callback(new Error('Bad envelop content'));
            }
            return callback(null, record);
        });
    }

    set(key, value, ttl, callback) {

        if (!this.client){
            return callback(new Error('Connection not started'));
        }

        const envelop = {
            PK: key.id,
            item: value,
            stored: Date.now(),
            ttl
        };

        const metadata = {
            ttl,
            gen: 1
        };

        const policy = {
            exists: Aerospike.policy.exists.CREATE_OR_REPLACE
        };

        try {
            JSON.stringify(envelop);
            this.client.put(this.generateKey(key), envelop, metadata, policy, (err, returnKey) => {

                if (err){
                    return callback(new Error('Error writing data'));
                }
                return callback();
            });
        }
        catch (e){
            return callback(e);
        }
    }

    drop(key, next) {

        if (!this.client){
            return next(new Error('Connection not started'));
        }

        this.client.remove(this.generateKey(key), (err, returnKey) => {

            if (err){
                return next(new Error('Error dropping item'));
            }
            return next();
        });
    }

    generateKey(key) {

        if (typeof key === 'object' && !Array.isArray(key)){
            if (!key.namespace){
                key.namespace = this._settings.partition;
            }
            if (!key.segment) {
                key.segment = this._settings.segment;
            }
            return new Aerospike.Key(key.namespace, key.segment, key.id);
        }
        else if (typeof key === 'string'){
            return new Aerospike.Key(this._settings.partition, this._settings.segment, key);
        }
        return new Error('Invalid Key');
    }
};
