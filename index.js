const redis = require('redis');
const util = require("util");
const EventEmitter = require("events").EventEmitter;
const debug = require('debug')('redis-pubsubber');
const EVENT_MSG = "event";
const ACK_MSG = "ack";
let callbackIDCounter = 0;
const callbacks = {};

module.exports = RedisPubSub;

function RedisPubSub(prefix, port, host) {
  if (!(this instanceof RedisPubSub)) return new RedisPubSub(prefix, port, host);

  const _self = this;
  const pub = redis.createClient(port, host);
  const sub = redis.createClient(port, host);
  sub.setMaxListeners(0);

  pub.on('error', onRedisError);
  sub.on('error', onRedisError);

  function onRedisError(err) {
    _self.emit("error", err);
  }

  this.createChannel = function (name) {
    const channel = new Channel(prefix + name, pub, sub);
    channel.on('error', onRedisError);
    return channel;
  };
}

util.inherits(RedisPubSub, EventEmitter);

function Channel(name, pub, sub) {
  const _self = this;
  sub.subscribe(name, function (err) {
    debug("subsribed to channel: ", name);
    if (err !== null) _self.emit("error", err);
  });

  sub.on("message", function (channel, packet) {
    if (channel !== name) return;
    packet = JSON.parse(packet);
    debug("received packet: ", packet);
    const data = packet.data;
    const callbackID = packet.id;
    switch (packet.type) {
      case EVENT_MSG:
        data.unshift("message"); // add event type in front
        if (callbackID !== undefined) {
          data.push(eventCallback); // add callback to end
        }
        _self.emit.apply(_self, data);

      function eventCallback() {
        if (callbackID === undefined) {
          return _self.emit("error", "No callback defined");
        }
        const args = Array.prototype.slice.call(arguments);
        const ackPacket = {
          type: ACK_MSG,
          data: args,
          id: callbackID
        };
        debug("publishing ack packet: ", ackPacket);
        pub.publish(name, JSON.stringify(ackPacket), function (err) {
          if (err !== null) _self.emit("error", err);
        });
      }

        break;
      case ACK_MSG:
        if (typeof callbacks[callbackID] === 'function') {
          callbacks[callbackID].apply(this, packet.data);
          delete callbacks[callbackID];
        }
        break;
    }
  });

  this.publish = function () {
    const args = Array.prototype.slice.call(arguments);
    const packet = {
      type: EVENT_MSG,
      data: args
    };
    // is there a callback function?
    if (typeof args[args.length - 1] === 'function') {
      packet.id = callbackIDCounter++;
      callbacks[packet.id] = packet.data.pop();
    }
    debug("publishing packet: ", packet);
    pub.publish(name, JSON.stringify(packet), function (err) {
      if (err !== null) _self.emit("error", err);
    });
    return packet.id;
  };

  this.removeCallback = (packetId) => {
    if (callbacks[packetId]) {
      delete callbacks[packetId];
    }
  };

  this.destroy = function () {
    sub.unsubscribe(name);
  };
}

util.inherits(Channel, EventEmitter);
