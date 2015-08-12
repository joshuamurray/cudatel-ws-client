/**
 * Primary Module Requirements
 */
var util = require( "util" );
var events = require( "events" );
var storage = require( "storage.json" );
var Connection = require ( "./connection" );

//TODO create bridge for cudatel-ws-db and cudatel-ws-client in stead of what is here now.

/**
 * The CudaTel tunnel connection module. Emits important WebSocket connection events.
 * @constructor
 */
var Client = function(){
    var $this = this;
    events.EventEmitter.call( this );

    /**
     * Listeners ( and stubs ) for events emitted from the Connection instance.
     * @type {{
     *     forged: Function,
     *     opened: Function,
     *     authed: Function,
     *     joined: Function,
     *     loaded: Function,
     *     booted: Function,
     *     bonded: Function,
     *     pulsed: Function,
     *     pinged: Function,
     *     tested: Function,
     *     closed: Function,
     *     pulled: Function,
     *     pushed: Function
     * }}
     */
    this.watchers = {

        /**
         * Initiation of module completed events.
         * Emitted by: Connection.start().
         */
        forged: function( channel, message ){ /*STUB*/ },

        /**
         * CudaTel WebSocket opened events.
         * Emitted after: Connection.on( "opening" ).
         */
        opened: function( channel, message ){ /*STUB*/ },

        /**
         * Authentication completed events.
         * Emitted after: Connection.on( "login" ).
         */
        authed: function( channel, message ){ /*STUB*/ },

        /**
         * Initiation channel connection procedural events.
         * Emitted by: Connection.channel.join().
         */
        joined: function( channel ){ /*STUB*/ },

        /**
         * Initiation channel connection procedural events.
         * Emitted by: Connection.channel.lock().
         */
        loaded: function( channel ){ /*STUB*/ },

        /**
         * Initiation channel connection procedural events.
         * Emitted after: Connection.on( "booting" ).
         */
        booted: function( channel ){ /*STUB*/ },

        /**
         * Channel ready events.
         * Emitted by: Connection.channel.bond().
         */
        bonded: function( channel, bootstrap ){ $this.fill( channel, bootstrap ); },

        /**
         * Outgoing Keep-alive message events.
         * Emitted by: Connection.channel.beat().
         */
        pulsed: function( channel ){ /*STUB*/ },

        /**
         * Incoming keep-alive message events.
         * Emitted by: Connection.commands.meteor_alive().
         */
        pinged: function( channel, message ){ /*STUB*/ },

        /**
         * Keep-alive status check events.
         * Emitted by: Connection.channel.test().
         */
        tested: function( channel ){ /*STUB*/ },

        /**
         * Connection closed events.
         * Emitted after: Connection.on( "closing" ).
         */
        closed: function( channel, message ){ $this.shut(); },

        /**
         * Messages sent from CudaTel events.
         * Emitted after: Connection.on( "reading" ).
         */
        pulled: function( channel, message ){ if( $this.done( channel )) $this.read( channel, message ); },

        /**
         * Message sent to Cudatel events.
         * Emitted by: Connection.send().
         */
        pushed: function( channel, message ){ /*STUB*/ }
    };

    /**
     * Initiates the connection. Once initiated, watches messages for those indexed in "this.watch".
     * Messages received from watched channels will be handled using "parser", "database", and
     * "output" modules. These will be handled according to the layout in the "config" file.
     * @param env
     */
    this.open = function(){
        $this.connection = new Connection( $this.domain );

        $this.wipe();

        $this.connection.start();
    };

    /**
     * Clears all data values from the instance to prepare for connection/re-connection.
     */
    this.wipe = function(){
        for( var event in $this.watchers ){
            $this.connection.on( event, $this.watchers[ event ]);
        }

        $this.watch = $this.connection.channel.sets.boot;

        for( var item in $this.watch ){
            var channel = $this.watch[ item ];
            $this.live[ channel ] = { data: [], ready: false };
        }
    };

    /**
     * Fires when the connection fires a "bonded" event for a watched channel.
     */
    this.fill = function( channel, bootstrap ){
        $this.live[ channel ].ready = true;

        for( var index in bootstrap ){
            $this.live[ channel ].data.push( bootstrap[ index ].data );
        }

        return $this.emit( "loaded", channel, $this.live[ channel ].data );
    };

    /**
     * Returns boolean indicating if all data has been received and channel is bootstrapped.
     * @param channel
     * @returns {boolean}
     */
    this.done = function( channel ){ return $this.live[ channel].ready ? true : false; };

    /**
     * Edits the live data array based on the message's "action",
     * then updates the data storage to contain the new values.
     * @param channel
     * @param message
     */
    this.read = function( channel, message ){
        var live = $this.live[ channel ];

        switch( message.action ){
            case "add":
                live.data[ message.data.row_id ] = message.data;
                return $this.emit( "create", channel, message.data, live.data );
            case "modify":
                live.data[ message.data.row_id ] = message.data;
                return $this.emit( "update", channel, message.data, live.data);
            case "del":
                live.data.splice( message.data.row_id, 1 );
                return $this.emit( "delete", channel, message.data, live.data);
            case "bootstrap_data":
                return $this.shut();
            default:
                return console.log( "UNKNOWN ACTION:", message.action );
        }

        return false;
    };

    /**
     * Creates a new Connection, when the instance fires a "closed" event.
     */
    this.shut = function(){
        console.error( "Connection closed.", "Re-opening." );

        $this.open();
    };

    /**
     * Loads existing config data from file OR creates the config data within the file.
     * @param relativeFilePath
     * @param callback
     */
    this.load = function( callback ){
        storage.file( "required.config" ).load( function( required ){
            storage.file( "./config" ).load( function( config ){
                var altered = [];
                var needToChangeValues = [];
                var loadName = "\"cudatel-ws-client\"";
                var errorMsg = "There was a problem loading the config file for "+ loadName +" module.";
                var alterMsg = loadName +" config options are located in config.json. Populate values before continuing.";

                for( var item in required ){
                    if( ! config[ item ]) altered.push( item );
                    else if( config[ item ] == required[ item ]) needToChangeValues.push( item );
                    else required[ item ] = config[ item ];
                }

                if( altered.length > 0 ) storage.save( required, function( saved ){
                    if( saved.def_env ) return console.log( alterMsg );
                    else return error( errorMsg );
                });
                else if( needToChangeValues.length > 0 ) return console.log( alterMsg, needToChangeValues );
                else callback( config );
            });
        });
    };

    /**
     * Bootstraps the tunnel connection and database based on the current config.json values.
     */
    this.boot = function(){
        $this.ids = [];
        $this.live = {};
        $this.load( function( config ){
            $this.env = config.def_env;
            $this.domain = config[ $this.env ].host;
        });
    };

    /**
     * Execute module bootstrap command.
     */
    $this.boot();

     /********************************************************
      * Stubs for events emitted by this module \/
     ********************************************************/

    /**
     * Record created in live data array.
     */
    $this.on( "create", function( channel, item_data, channel_data ){ /* STUB */ });

    /**
     * Record updated in live data array.
     */
    $this.on( "update", function( channel, item_data, channel_data ){ /* STUB */ });

    /**
     * Record deleted from live data array.
     */
    $this.on( "delete", function( channel, item_data, channel_data ){ /* STUB */ });

    /**
     * Several records were changed in the database.
     */
    $this.on( "loaded", function( channel, channel_data ){ /* STUB */ });
};

/**
 * Client inheritance from events.EventEmitter
 * @type {Object|Function|exports.EventEmitter}
 * @private
 */
util.inherits( Client, events.EventEmitter );

/**
 * Called PRIOR TO export to ensure that all variable values from "config.json"
 * have been loaded. Sets all initial variables to their respective values.
 * Instantiates any modules requiring instantiation, prior to exporting.
 */
var bootstrap = function(){};

/**
 * Execute the bootstrap command before exporting
 */
bootstrap();

/**
 * Exports an instantiation of the Client module.
 */
module.exports = Client;

