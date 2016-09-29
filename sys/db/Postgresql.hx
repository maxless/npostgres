/*
 * Copyright (c) 2006, DesignRealm.co.uk
 * All rights reserved.
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   - Redistributions of source code must retain the above copyright
 *     notice, this list of conditions and the following disclaimer.
 *   - Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE HAXE PROJECT CONTRIBUTORS "AS IS" AND ANY
 * EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL THE HAXE PROJECT CONTRIBUTORS BE LIABLE FOR
 * ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
 * SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
 * CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
 * OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH
 * DAMAGE.
 *
 * Written by Lee McColl Sylvester
 * Modifications by Max S
 *
 */
#if haxe3
package sys.db;

import sys.db.Connection;
#else
package neko.db;

import neko.db.Connection;
#end

#if neko
import neko.Lib;
#elseif cpp
import cpp.Lib;
#end

import sys.net.Socket;


class PostgresResultSet implements ResultSet {

#if haxe3
    public var length(get,null) : Int;
    public var nfields(get,null) : Int;
#else
    public var length(get_length,null) : Int;
    public var nfields(get_nfields,null) : Int;
#end
    var r : Dynamic;
    var cache : Dynamic;

    public function new( r ) {
        this.r = r;
    }


    public function getFieldsNames() : Null<Array<String>> {
        if (nfields == 0)
            return null;

        var list = [];
        for (i in 0...nfields)
            list.push('' + result_get_column_name(r, i));

        return list;
    }

    function get_length() {
        return result_get_length( r );
    }

    function get_nfields() {
        return result_get_nfields( r );
    }

    public function hasNext() {
        if( cache == null )
            cache = next();
        return ( cache != null );
    }

    public function next() : Dynamic {
        var c = cache;
        if( c != null ) {
            cache = null;
            return c;
        }
        c = result_next( r );
        if( c == null )
            return null;
#if neko
        untyped {
            var f = __dollar__objfields( c );
            var i = 0;
            var l = __dollar__asize( f );
            while( i < l ) {
                var v = __dollar__objget( c, f[i] );
                if( __dollar__typeof( v ) == __dollar__tstring )
                    __dollar__objset( c, f[i], new String( v ) );
                i = i + 1;
            }
        }
#end
        return c;
    }

    public function results() : List<Dynamic> {
        var l = new List();
        while( hasNext() )
            l.add( next() );
        return l;
    }

    public function getResult( n : Int ) {
        return new String( result_get( r, n ) );
    }

    public function getIntResult( n : Int ) : Int {
        return result_get_int( r, n );
    }

    public function getFloatResult( n : Int ) : Float {
        return result_get_float( r, n );
    }

    public function lastInsertId() : Int {
        return result_last_id( r );
    }

    // get error message of this result, returns empty string on ok
    public function getErrorMessage() {
        return new String( result_get_error( r ) );
    }

    static var result_next = Lib.load("npostgres","np_result_next",1);
    static var result_get_length = Lib.load("npostgres","np_result_get_length",1);
    static var result_get_nfields = Lib.load("npostgres","np_result_get_nfields",1);
    static var result_get = Lib.load("npostgres","np_result_get",2);
    static var result_get_int = Lib.load("npostgres","np_result_get_int",2);
    static var result_get_float = Lib.load("npostgres","np_result_get_float",2);
    static var result_last_id = Lib.load("npostgres","np_last_insert_id",1);
    static var result_get_error = Lib.load("npostgres","np_result_get_error",1);
    static var result_get_column_name = Lib.load("npostgres","np_result_get_column_name",2);

}

class PostgresConnection implements Connection {

    private var __c : Dynamic;
    private var socket : PostgresSocket;
    private var id : Int;

    public function new( c ) {
        __c = _connect( c );
        socket = null;
    }

    public function request( qry : String ) : ResultSet {
#if neko
        var r = _request( __c, untyped qry.__s );
#elseif cpp
        var r = _request( __c, qry);
#end

        var rs : ResultSet = new PostgresResultSet( r );
        id = cast( rs, PostgresResultSet ).lastInsertId();
        return rs;
    }

    public function close() {
        _close( __c );
    }

    public function escape( s : String ) {
        return s.split( "\\" ).join( "\\\\" ).split( "'" ).join( "\\'" );
    }

    public function quote( s : String ) {
        return "E'"+escape( s )+"'";
    }


    public function addValue( s : StringBuf, v : Dynamic ) {
#if neko
        var t = untyped __dollar__typeof(v);
        if( untyped (t == __dollar__tint || t == __dollar__tnull) )
            s.add(v);
        else if( untyped t == __dollar__tbool )
            s.add(if( v ) "'t'" else "'f'");
        else {
            s.add("E'");
            s.add(escape(Std.string(v)));
            s.addChar("'".code);
        }
#elseif cpp
        if (v == null)
            s.add(v);

        if(Std.is(v, Int) || Std.is(v, Float))
            s.add(v);
        else if(Std.is(v, Bool))
            s.add(if( v ) "'t'" else "'f'");
        else {
            s.add("E'");
            s.add(escape(Std.string(v)));
            s.addChar("'".code);
        }
#end
    }

    public function lastInsertId() {
        return id;
    }

    public function dbName() {
        return "PostgreSQL";
    }

    public function startTransaction() {
        request("BEGIN TRANSACTION");
    }

    public function commit() {
        request("COMMIT");
    }

    public function rollback() {
        request("ROLLBACK");
    }
/*
    public function hasFeature( f ) {
        switch( f )
        {
            case "ForUpdate": return false;
        }
        return false;
    }
*/

/**
  From the PostgreSQL documentation:

    Sets the nonblocking status of the connection.

    int PQsetnonblocking(PGconn *conn, int arg);

    Sets the state of the connection to nonblocking if arg is 1, or blocking if arg is 0. Returns 0 if OK, -1 if error.

    In the nonblocking state, calls to PQsendQuery, PQputline, PQputnbytes, and PQendcopy will not block but instead return an error if they need to be called again.
**/
  public function setNonBlocking(v: Bool): Int
    {
      return _set_non_blocking(__c, (v ? 1 : 0));
    }

/**
  From the PostgreSQL documentation:

    Returns the blocking status of the database connection.

    int PQisnonblocking(const PGconn *conn);

    Returns 1 if the connection is set to nonblocking mode and 0 if blocking.
**/
  public function isNonBlocking(): Bool
    {
      return _is_non_blocking(__c);
    }

/**
  From the PostgreSQL documentation:

    Obtains the file descriptor number of the connection socket to the server. A valid descriptor will be greater than or equal to 0; a result of -1 indicates that no server connection is currently open. (This will not change during normal operation, but could change during connection setup or reset.)

    int PQsocket(const PGconn *conn);
**/
  public function getSocket(): Socket
    {
      if (socket != null)
        return socket;

      // hack: open dummy socket to get proper k_socket "kind" on lower level
      var tmp = _socket_new(false);
      var __s: Dynamic = _get_socket(__c, tmp);
      if (__s == -1)
        return null;

      socket = new PostgresSocket(untyped __s);

      return socket;
    }

/**
  From the PostgreSQL documentation:

    Attempts to flush any queued output data to the server. Returns 0 if successful (or if the send queue is empty), -1 if it failed for some reason, or 1 if it was unable to send all the data in the send queue yet (this case can only occur if the connection is nonblocking).

    int PQflush(PGconn *conn);

    After sending any command or data on a nonblocking connection, call PQflush. If it returns 1, wait for the socket to become read- or write-ready. If it becomes write-ready, call PQflush again. If it becomes read-ready, call PQconsumeInput, then call PQflush again. Repeat until PQflush returns 0. (It is necessary to check for read-ready and drain the input with PQconsumeInput, because the server can block trying to send us data, e.g. NOTICE messages, and won't read our data until we read its.) Once PQflush returns 0, wait for the socket to be read-ready and then read the response as described above.
**/
  public function flush(): Int
    {
      return _flush(__c);
    }

/**
  From the PostgreSQL documentation:

    Submits a command to the server without waiting for the result(s). 1 is returned if the command was successfully dispatched and 0 if not (in which case, use PQerrorMessage to get more information about the failure).

    int PQsendQuery(PGconn *conn, const char *command);

    After successfully calling PQsendQuery, call PQgetResult one or more times to obtain the results. PQsendQuery cannot be called again (on the same connection) until PQgetResult has returned a null pointer, indicating that the command is done.
**/
  public function sendQuery(qry: String): Bool
    {
#if neko
        var ret = _send_query( __c, untyped qry.__s );
#elseif cpp
        var ret = _send_query( __c, qry);
#end

        return ret;
    }

/**
  From the PostgreSQL documentation:

    Waits for the next result from a prior PQsendQuery, PQsendQueryParams, PQsendPrepare, or PQsendQueryPrepared call, and returns it. A null pointer is returned when the command is complete and there will be no more results.

    PGresult *PQgetResult(PGconn *conn);

    PQgetResult must be called repeatedly until it returns a null pointer, indicating that the command is done. (If called when no command is active, PQgetResult will just return a null pointer at once.) Each non-null result from PQgetResult should be processed using the same PGresult accessor functions previously described. Don't forget to free each result object with PQclear when done with it. Note that PQgetResult will block only if a command is active and the necessary response data has not yet been read by PQconsumeInput.
**/

  public function getResult() : ResultSet
    {
      var r = _get_result(__c);
      if (r == null)
        return null;

      var res: ResultSet = new PostgresResultSet(r);
      id = cast(res, PostgresResultSet).lastInsertId();
      return res;
    }

/**
  From the PostgreSQL documentation:

    If input is available from the server, consume it.

    int PQconsumeInput(PGconn *conn);

    PQconsumeInput normally returns 1 indicating "no error", but returns 0 if there was some kind of trouble (in which case PQerrorMessage can be consulted). Note that the result does not say whether any input data was actually collected. After calling PQconsumeInput, the application can check PQisBusy and/or PQnotifies to see if their state has changed.

    PQconsumeInput can be called even if the application is not prepared to deal with a result or notification just yet. The function will read available data and save it in a buffer, thereby causing a select() read-ready indication to go away. The application can thus use PQconsumeInput to clear the select() condition immediately, and then examine the results at leisure.
**/
  public function consumeInput(): Bool
    {
      return _consume_input(__c);
    }


/**
  From the PostgreSQL documentation:

    Returns 1 if a command is busy, that is, PQgetResult would block waiting for input. A 0 return indicates that PQgetResult can be called with assurance of not blocking.

    int PQisBusy(PGconn *conn);

    PQisBusy will not itself attempt to read data from the server; therefore PQconsumeInput must be invoked first, or the busy state will never end.
**/
  public function isBusy(): Bool
    {
      return _is_busy(__c);
    }


/**
    Returns the error message most recently generated by an operation on the connection.

    char *PQerrorMessage(const PGconn *conn);

    Nearly all libpq functions will set a message for PQerrorMessage if they fail. Note that by libpq convention, a nonempty PQerrorMessage result can consist of multiple lines, and will include a trailing newline. The caller should not free the result directly. It will be freed when the associated PGconn handle is passed to PQfinish. The result string should not be expected to remain the same across operations on the PGconn structure.
**/
  public function getErrorMessage(): String
    {
      return new String( _error_message(__c) );
    }


  static var _connect = neko.Lib.load("npostgres","np_connect",1);
  static var _close = neko.Lib.load("npostgres","np_free_connection",1);
  static var _request = Lib.load("npostgres","np_request",2);

  static var _set_non_blocking = neko.Lib.load("npostgres","np_set_non_blocking",2);
  static var _is_non_blocking = neko.Lib.load("npostgres","np_is_non_blocking",1);
  static var _get_socket = neko.Lib.load("npostgres","np_get_socket",2);
  static var _flush = neko.Lib.load("npostgres","np_flush",1);
  static var _send_query = neko.Lib.load("npostgres","np_send_query",2);
  static var _get_result = neko.Lib.load("npostgres","np_get_result",1);
  static var _consume_input = neko.Lib.load("npostgres","np_consume_input",1);
  static var _is_busy = neko.Lib.load("npostgres","np_is_busy",1);
  static var _error_message = neko.Lib.load("npostgres","np_error_message",1);

  static var _socket_new = Lib.load("std","socket_new",1);
}


private class PostgresSocket extends Socket
{
  public function new (s: Dynamic)
    {
#if neko
      __s = untyped s;
      super();
#elseif cpp
      // NOTE: this hack is done due to the Socket.hx constructor being
      // different in Neko and HXCPP. We also set input and output to null
      // because SocketInput and SocketOutput classes are private.
      super();
      __s = untyped s;
      input = null;
      output = null;
#end
    }
}


class Postgresql
{
  public static function open( conn : String ) : Connection
    {
#if neko
        return new PostgresConnection( untyped conn.__s );
#elseif cpp
        return new PostgresConnection( conn );
#end
    }
}
