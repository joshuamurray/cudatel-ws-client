/**
 * Primary Module Requirements
 */
var WebSocket = require( 'ws' );
var events = require( "events" );
var errors = require( "./error" );
var ajax = require( "cudatel-ajax" );
var storage = require( "storage.json" );

/**
 * Extends the standard Object to enable swiftly counting its properties.
 * Returns the total number of properties within an object when called.
 * @param obj
 * @returns {number}
 */
Object.size = function( object ){
    var size = 0;

    for( var key in object ){ if( object.hasOwnProperty( key )) size++; }

    return size;
};

/**
 * Handles the WebSocket tunnel connection, including instantiation, authentication, and connection to channels.
 * Emits: "forged", "opened", "authed", "joined", "booted", "bonded", "pinged", "pulsed", "tested", and "closed".
 * Message event emitters: "pushed" and "pulled".
 *
 * @param domain - Domain for the Cudatel Server ( *Without http/https/ws/wss : cudatel.mydomain.com / 10.0.0.2 / etc. )
 * @constructor
 */
var Connection = function( env ){
    var $this = this;
    events.EventEmitter.call( this );

    /**
     * Object containing channel handlers and bindings.
     */
    this.channel = {

        /**
         * The count for the chl property on messages as they are sent.
         */
        poll: 0,

        /**
         * The channels that will be joined by the connection.
         * @prop join = [] - Join only.
         * @prop boot = [] - Join AND bootstrap.
         */
        sets: {},

        /**
         * Channel bindings to update the status of the connected channels.
         */
        data: { join: {}, boot: {} },

        /**
         * List the channels within a specific type ("boot", "join"), no args
         * returns an associative object containing all channel set arrays.
         * @param type
         * @returns {*}
         */
        list: function( type ){
            if( ! type || ! $this.channel.sets[ type ] ) return $this.channel.sets;

            return $this.channel.sets[ type ];
        },

        /**
         * Returns the "type" of the channel based on the channel name string.
         * @param channel
         * @returns {*}
         */
        type: function( channel ){
            for( var type in $this.channel.sets ){
                if( $this.channel.sets[ type ].indexOf( channel ) != -1 ) return type;
            }

            return $this.emit( "error", "Can't locate channel " + channel + " for type." );
        },

        /**
         * Sends all channel join messages for all channel sets.
         * @param sent
         * @returns {boolean}
         */
        join: function( sent ){
            var list = $this.channel.list();

            $this.channel.each( $this.channel.make );

            $this.send( "join", list.join );
            $this.send( "join", list.boot );

            $this.channel.each( function( channel ){
                if( $this.channel.data[ channel ]) $this.emit( "joined", channel );
            });

            return false;
        },

        /**
         * Asserts channel connection is complete
         * Join Channel - joined successfully (boolean)
         * Boot Channel - booted successfully (boolean)
         * No Arguments - All channels joined and booted successfully (boolean)
         * @param channel
         * @returns {*}
         */
        done: function( channel ){
            channel = channel || false;

            if( ! channel ){
                $this.channel.each( function( channel ){
                    if( ! $this.channel.data[ channel ].done ) return false;
                });

                return true;
            } else{
                return $this.channel.data[ channel ].done;
            }
        },

        /**
         * Sets a channels state based on the type of the channel.
         * @param channel
         * @returns {*}
         */
        lock: function( channel ){
            var load = $this.channel.data[ channel ];

            if( $this.channel.type( channel ) == 'boot' && load && ! load.sent ) return $this.emit( "booting", channel );

            $this.channel.data[ channel].done = true;

            return $this.emit( "loaded", channel );
        },

        /**
         * Increments this.poll for use in sent messages.
         * @returns { number }
         */
        next: function(){ return $this.channel.poll++; },

        /**
         * Performs a callback function on each channel within channel.sets regardless of type.
         * @param callback
         */
        each: function( callback ){
            for( var type in $this.channel.sets ){
                var channels = $this.channel.sets[ type ];
                channels.forEach( callback );
            }
        },

        /**
         * Creates a new channel binding for the channel.
         * @param channel
         * @returns {{ beat: number, tick: number, test: number, user: Array, done: boolean, sent: boolean }}
         */
        make: function( channel ){
            var new_channel = { beat: 0, tick: 0, test: 0, user: [], done: false, sent: true };

            if( $this.channel.type( channel ) == 'boot' ) new_channel.sent = false;

            $this.channel.data[ channel ] = new_channel;

            return $this.channel.data[ channel ];
        },

        /**
         * Returns the channel name from a raw message object.
         * @param message
         * @returns {*}
         */
        find: function( message ){
            if( message.data.channel ) return message.data.channel.toLowerCase();
            if( message.data.pipe ) return message.data.pipe.properties.name.toLowerCase();

            return false;
        },

        /**
         * Initiates keep-alive message intervals to maintain connection.
         * @param channel
         */
        flow: function( channel ){
            $this.channel.data[ channel ].beat = setInterval( function(){ $this.channel.beat( channel ); }, 5000);
            $this.channel.data[ channel ].test = setInterval( function(){ $this.channel.test( channel ); }, 30000);
        },

        /**
         * Clears intervals and resets data within the channel binding.
         * @param channel
         * @returns {*}
         */
        dump: function( channel ){
            if( ! $this.channel.data[ channel ] ) return $this.emit( "error", "Can't dump unknown channel : " + channel );
            else clearInterval( $this.channel.data[ channel ].beat );

            $this.channel.make( channel );
        },

        /**
         * Increments the heartbeat id for use in heartbeat messages.
         * @param channel
         * @returns {number}
         */
        tick: function( channel ){ return $this.channel.data[ channel ].tick++; },

        /**
         * Sends a heartbeat message within the scope of the channel.
         * @param channel
         */
        beat: function( channel ){ $this.send( "beat", channel ); $this.emit( "pulsed", channel ); },

        /**
         * Sends a authentication check message within the scope of the channel.
         * @param channel
         */
        test: function( channel ){ $this.send( "stat", channel ); $this.emit( "tested", channel ); }
    };

    /**
     * Instantiates the tunnel and makes teh connection. Associates listeners with the WebSocket's events.
     */
    this.start = function(){
        $this.tunnel = new WebSocket( $this.option.url, $this.option.opt );

        for( var event in $this.socketEvents ){
            var eventHandler = $this.socketEvents[ event ];
            $this.tunnel.on( event, eventHandler );
        }

        $this.emit( "forged", $this.option.url );
    };

    /**
     * Fired before starting the connection to set all variable values.
     */
    this.bootstrap = function(){
        $this.bond = {};
        $this.no_calls = {};
        storage.file( "./config").load( "cudatel_ws_client", function( config ){
            $this.config = config;
            $this.env = config.def_env;
            $this.domain = config[ $this.env ].host;
            $this.option = {
                url: "ws://" + $this.domain + "/6/",
                opt: { origin: "http://" + $this.domain + "/" }
            }
            $this.channel.sets =  config.sets;
        });
    };

    /**
     * Checks if calls exist in the initiation message, if there are no active calls, then this.no_calls is set to true.
     * @param messages
     */
    this.assemble = function( messages ){
        var channel;
        var boot = false, init = false, clear = false;

        messages.forEach( function( message, index ){
            var chlName = $this.channel.find( message );

            if( ! channel && chlName ) channel = chlName;

            var action = message.data.data && message.data.data.action ? message.data.data.action : false;
            if( action == "bootstrap_data" ) boot = true;
            if( action == "init" ) init = true;
            if( action == "clear" ) clear = true;
        });

        if( clear && ! channel ) return $this.emit( "error", "Can't assemble bootstrap without channel.", messages );

        $this.no_calls[ channel ] = true;

        if( init && clear && boot ) $this.no_calls[ channel ] = false;
    };

    /**
     * Formats the message by creating an associative array, using the stored column names
     * and message values. Returns the message if channel did not provide column names.
     * @param message
     */
    this.format = function( message, channel ){
        if( ! this.bond[ channel ] || $this.bond[ channel ].length === 0 ) return false;

        var data = message.data.data;

        var record = {
            data: {},
            message: message,
            cols: $this.bond[ channel ],
            call: { aleg: {}, bleg: {}},
            action: data.action || "bootstrap"
        };

        for( var position in data.data ){
            var value = data.data[ position ];
            var label = record.cols[ position ] || "FAILED";
            var leg = label.substring( 0, 2 );
            var lab = label.substring( 0, 1 ) + 'leg';
            var col = label.substring( 2, label.length );

            if( leg == 'a_' || leg == 'b_' ) record.call[ lab ][ col ] = value;
            else record.call[ label ] = value;

            record.data[ label ] = value;
        }

        record.data.id = ( parseInt( record.data.row_id ) + 1 ).toString();

        return record;
    };

    /**
     * Handlers for the WebSocket-emitted events, fired when the corresponding event is emitted by $this.tunnel
     * @type {{open: Function, close: Function, error: Function, message: Function}}
     */
    this.socketEvents = {

        /**
         * Fires when the tunnel connection is opened.
         */
        open: function(){ $this.emit( "opening" ); },

        /**
         * Fires when the tunnel connection is closed.
         */
        close: function(){ $this.emit( 'closing' ); },

        /**
         * Fired when an error occurs within the tunnel connection.
         * @param error
         * @param description
         */
        error: function( error, description ){ $this.emit( 'error', error, description ); },

        /**
         * Fired when a message is received from the tunnel connection.
         * @param data
         * @param flags
         */
        message: function( data, flags ){ $this.emit( "reading", JSON.parse( data )); }
    };

    /**
     * Setter method for "pubid"
     * @param pubid
     */
    this.setPubId = function( pubid ){ $this.pubid = pubid; };

    /**
     * Sets user data bindings for the channel
     * @param userData
     */
    this.setUsers = function( channel, userData ){ $this.channel.data[ channel].user = userData; };

    /**
     * Tunnel - Incoming "open" events.
     */
    $this.on( "opening", function(){
        ajax.open( $this.config[ $this.env ].user, function( sessionId, data ){
            $this.send( "auth", sessionId );

            return $this.emit( "opened", $this.config.ws_sessid );
        });
    });

    /**
     * Tunnel - Incoming "close" events.
     */
    $this.on( "closing", function(){
        for( var type in $this.channel.sets ){
            var channels = $this.channel.sets[ type ];

            channels.forEach( $this.channel.dump );
        }

        ajax.shut( function( sessionId, loggedOut ){
           if( loggedOut ) $this.emit( "closed" );
        });
    });

    /**
     * Tunnel - Incoming "message" events.
     */
    $this.on( "reading", function( messages ){
        $this.assemble( messages );

        messages.forEach( function( message, index ){
            var channel = $this.channel.find( message );

            if( ! channel || $this.commands.hasOwnProperty( message.raw.toLowerCase()) || ! $this.channel.done( channel )) return $this.emit( "command", channel, message );

            var formatted = $this.format( message, channel );

            return $this.emit( "pulled", channel, formatted );
        });
    });

    /**
     * Initiation channel data procedural events
     */
    $this.on( "booting", function( channel ){
        var booted = true;

        if( ! $this.channel.data[ channel ].sent ){
            $this.send( 'boot', channel );
            $this.channel.data[ channel ].sent = true;
        }

        $this.channel.list( "boot" ).forEach( function( channel_name ){
            var channel_data = $this.channel.data[ channel_name ];
            if( ! channel_data || ! channel_data.sent ) booted = false;
        });

        if( booted ) return $this.emit( "booted", channel );
    });

    /**
     * Initatition general procedural events.
     */
    $this.on( "command", function( channel, message ){
        var data = message.data.data || message.data;
        var action = data.action || message.raw;
        var act = ( channel == 'meteor_alive' ? channel : action ).toLowerCase();

        var command = $this.commands[ act ] || $this.commands.default;

        return command( channel, message );
    });

    /**
     * Initiation structure message events.
     */
    $this.on( "build", function( channel, message ){
        $this.bond[ channel ] = message.data.data.data;

        if( $this.no_calls[ channel ]) $this.emit( "strap", channel, { data:{ data:{ data:[]}}});
    });

    /**
     * Initiation notification message events.
     */
    $this.on( "clear", function( channel, message ){ $this.bond[ channel ] = []; });

    /**
     * Initiation data message events.
     */
    $this.on( "strap", function( channel, message ){
        $this.channel.flow( channel );
        $this.channel.lock( channel );

        var bootstrap = [];
        var rows = message.data.data.data;

        for( var row in rows ){
            var copy = message;
            copy.data.data.data = rows[ row ];
            bootstrap.push( $this.format( copy, channel ));
        }

        $this.emit( "bonded", channel, bootstrap );
    });

    /**
     * Authentication message events.
     */
    $this.on( "login", function( channel, message ){
        $this.config.ws_sessid = message.data.sessid;
        storage.file( "./config" ).save( "cudatel_ws_client", $this.config, function( saved ){
            $this.emit( "authed", $this.config.ws_sessid );

            $this.channel.join();
        });
    });

    /**
     * User presence events.
     */
    $this.on( "users", function( channel, message ){
        var action = message.raw.toLowerCase();

        if( action == "ident" ) return $this.setPubId( message.data.user.pubid );
        if( action == "channel" ) return $this.setUsers( message.data.users );

        var user = message.data.user.pubid;
        var indx = $this.channel.data[ channel].user.indexOf( user );

        if( action == "join" ) return $this.channel.data[ channel].user.push( user );
        if( action == "left" ) return $this.channel.data[ channel].user.splice( indx, 1 );
    });

    /**
     * Internal and incoming "error" events.
     */
    $this.on( "error", function( error, code ){
        return errors( error, code );
    });

    /**
     * Handlers for incoming messages. These handle all messages that do not have their
     * own channel, as well as channel messages that are received during initiation.
     * @type {{
     *     join_channel: Function,
     *     login: Function,
     *     join: Function,
     *     left: Function,
     *     ident: Function,
     *     channel: Function,
     *     meteor_alive: Function,
     *     init: Function,
     *     clear: Function,
     *     bootstrap_data: Function,
     *     err: Function,
     *     default: Function
     * }}
     */
    this.commands = {

        /**
         * Handles messages confirming the joining of a channel.
         * @param channel
         * @param message
         * @returns {*}
         */
        join_channel: function( channel, message ){ return $this.channel.lock( channel ); },

        /**
         * Handles authentication messages.
         * @param channel
         * @param message
         * @returns {*}
         */
        login: function( channel, message ){ return $this.emit( "login", channel, message ); },

        /**
         * Handles messages regarding user presence ( JOINING CHANNEL - ANY USER )
         * @param channel
         * @param message
         * @returns {*}
         */
        join: function( channel, message ){ return $this.emit( "users", channel, message ); },

        /**
         * Handles messages regarding user presence ( LEAVING CHANNEL - ANY USER )
         * @param channel
         * @param message
         * @returns {*}
         */
        left: function( channel, message ){ return $this.emit( "users", channel, message ); },

        /**
         * Handles messages regarding user presence ( CURRENT USER )
         * @param channel
         * @param message
         * @returns {*}
         */
        ident: function( channel, message ){ return $this.emit( "users", channel, message ); },

        /**
         * Handles messages regarding user presence ( ALL USERS )
         * @param channel
         * @param message
         * @returns {*}
         */
        channel: function( channel, message ){ return $this.emit( "users", channel, message ); },

        /**
         * Handles messages from the "meteor_alive" channel. ( Keep-Alives );
         * @param channel
         * @param message
         * @returns {*}
         */
        meteor_alive: function( channel, message ){ return $this.emit( "pinged", channel, message ); },

        /**
         * Handles messages containing initial channel property names.
         * @param channel
         * @param message
         * @returns {*}
         */
        init: function( channel, message ){ return $this.emit( "build", channel, message ); },

        /**
         * Handles messages commanding channel data reset.
         * @param channel
         * @param message
         * @returns {*}
         */
        clear: function( channel, message ){ return $this.emit( "clear", channel, message ); },

        /**
         * Handles messages containing initial channel property values.
         * @param channel
         * @param message
         * @returns {*}
         */
        bootstrap_data: function( channel, message ){ return $this.emit( "strap", channel, message ); },

        /**
         * Handles messages containing errors sent through the tunnel.
         * @param channel
         * @param message
         * @returns {*}
         */
        err: function( channel, message ){ return $this.emit( "error", message.data.value, message.data.code ); },

        /**
         * Default Handler - Handles messages that are not otherwise handled.
         * @param channel
         * @param message
         * @returns {*}
         */
        default: function( channel, message ){ return $this.emit( "error", "Uncategorized message from" + channel + "received." ); }
    };

    /**
     * Sends a message object through the tunnel after formatting it as a JSON string
     * @param message
     * @param onError
     */
    this.send = function( format, context ){
        var message = $this.write( format, context );

        msgString = JSON.stringify([message]);

        $this.tunnel.send( msgString, function( error ){
            if( typeof error != 'undefined' ) $this.emit( "error", error );
            else $this.emit( "pushed", message );
        });
    };

    /**
     * Assembles an outgoing message based on action and context. The returned
     * message ( JSON String ) is ready to send through the WebSocket tunnel
     * @param action
     * @param context
     * @returns {{cmd: string, chl: number, sessid: boolean, params: *}}
     */
    this.write = function( format, context ){
        var formats = { stat: "CHECK", auth: "CONNECT", join: "JOIN", boot: "BROADCAST", beat: "BROADCAST" };

        var data = {
            cmd: formats[ format ],
            sessid: $this.config.ws_sessid,
            chl: $this.channel.next(),
            params: $this.writers[ format ]( context )
        };

        if( format == "auth" ) delete data.sessid;

        return data;
    };

    /**
     * Message writer functions. These are executed when locating
     * the "params" value for the message currently being sent.
     * @type {{connect: Function, join: Function, broadcast: Function}}
     */
    this.writers = {
        stat: function(){ return {}; },
        join: function( channel ){ return { channels: channel }; },
        auth: function( session ){ return { session: session }; },
        boot: function( context ){ return $this.advanced( 'bootstrap', context ); },
        beat: function( context ){ return $this.advanced( 'heartbeat', context ); }
    };

    /**
     * Assembles params for advanced requests, to be sent through the tunnel
     * @param command
     * @param context
     * @param options
     * @returns {{command: *, context: *, obj: *}}
     */
    this.advanced = function( command, context, options ){
        var params = { data:{}};
        var obj = {
            page_size:25,
            distinct_on:"",
            allow_null_distinct:"",
            order_by: "",
            search: {},
            ident: "default"
        };

        if( command == 'heartbeat' ) obj.hb_id = $this.channel.tick( context );

        params.data.liveArray = {
            command: command,
            context: context,
            obj: obj
        };

        return params;
    };

    /**************************************************************************
      Event emitter stubs for use in those requiring this module.
    **************************************************************************/

    /**
     * Initiation of module completed events.
     * Emitted by: Connection.start().
     */
    $this.on( "forged", function( channel, message ){});

    /**
     * CudaTel WebSocket opened events.
     * Emitted after: Connection.on( "opening" ).
     */
    $this.on( "opened", function( channel, message ){});

    /**
     * Authentication completed events.
     * Emitted after: Connection.on( "login" ).
     */
    $this.on( "authed", function( channel, message ){});

    /**
     * Initiation channel connection procedural events.
     * Emitted by: Connection.channel.join().
     */
    $this.on( "joined", function( channel ){});

    /**
     * Initiation channel connection procedural events.
     * Emitted by: Connection.channel.lock().
     */
    $this.on( "loaded", function( channel ){});

    /**
     * Initiation channel connection procedural events.
     * Emitted after: Connection.on( "booting" ).
     */
    $this.on( "booted", function( channel ){});

    /**
     * Channel ready events.
     * Emitted by: Connection.channel.bond().
     */
    $this.on( "bonded", function( channel, bootstrap ){});

    /**
     * Outgoing Keep-alive message events.
     * Emitted by: Connection.channel.beat().
     */
    $this.on( "pulsed", function( channel ){});

    /**
     * Incoming keep-alive message events.
     * Emitted by: Connection.commands.meteor_alive().
     */
    $this.on( "pinged", function( channel, message ){});

    /**
     * Keep-alive status check events.
     * Emitted by: Connection.channel.test().
     */
    $this.on( "tested", function( channel ){});

    /**
     * Connection closed events.
     * Emitted after: Connection.on( "closing" ).
     */
    $this.on( "closed", function( channel, message ){});

    /**
     * Messages sent from CudaTel events.
     * Emitted after: Connection.on( "reading" ).
     */
    $this.on( "pulled", function( channel, message ){});

    /**
     * Message sent to Cudatel events.
     * Emitted by: Connection.send().
     */
    $this.on( "pushed", function( channel, message ){});

    /**
     * Execute the module bootstrap command.
     */
    $this.bootstrap();
};

/**
 * Connection inheritance from events.EventEmitter
 * @type {Object|Function|exports.EventEmitter}
 * @private
 */
Connection.setPrototypeOf( events.EventEmitter.getPrototypeOf());

/**
 * Exporting the Connection module
 * @type {connection|*}
 */
module.exports = Connection;
