module.exports = function( errorMessage, errorCode ){
    var err = new Error( errorMessage, errorCode );

    console.error( errorMessage );

    throw err;
}
