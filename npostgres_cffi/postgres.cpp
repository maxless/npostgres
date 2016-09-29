/*
 *  Copyright 2006 - Lee McColl Sylvester
 *  All rights reserved
 *  Redistribution and use in source and binary forms, with or without
 *  modification, are permitted provided that the following conditions are met:
 *
 *  - Redistributions of source code must retain the above copyright
 *  notice, this list of conditions and the following disclaimer.
 *  - Redistributions in binary form must reproduce the above copyright
 *  notice, this list of conditions and the following disclaimer in the
 *  documentation and/or other materials provided with the distribution.
 *
 *  THIS SOFTWARE IS PROVIDED BY "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES,
 *  INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY
 *  AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL
 *  LEE MCCOLL SYLVESTER BE LIABLE FOR  ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
 *  EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
 *  PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS;
 *  OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
 *  WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR
 *  OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF
 *  ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 * Written by Lee McColl Sylvester
 * Modifications by Max S
 *
 */

#define IMPLEMENT_API
#include <hx/CFFI.h>
#include <hx/NekoFunc.h>
#include <hxcpp.h>
#include <hx/StdLibs.h>

#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include "libpq-fe.h"

#define PGCONN( o )     ((connection*)val_data( o ))
#define PGRESULT( o )   ((result*)val_data( o ))

typedef struct {
    PGconn *m;
} connection;

DEFINE_KIND( k_connection );
DEFINE_KIND( k_result );

#undef CONV_FLOAT

typedef enum {
    BOOL_OID = 16,
    BYTEA_OID = 17,
    CHAR_OID = 18,
    NAME_OID = 19,
    INT8_OID = 20,
    INT2_OID = 21,
    INT2VECTOR_OID = 22,
    INT4_OID = 23,
    REGPROC_OID = 24,
    TEXT_OID = 25,
    OID_OID = 26,
    TID_OID = 27,
    XID_OID = 28,
    CID_OID = 29,
    OIDVECTOR_OID = 30,
    PG_TYPE_RELTYPE_OID = 71,
    PG_ATTRIBUTE_RELTYPE_OID = 75,
    PG_PROC_RELTYPE_OID = 81,
    PG_CLASS_RELTYPE_OID = 83,
    POINT_OID = 600,
    LSEG_OID = 601,
    PATH_OID = 602,
    BOX_OID = 603,
    POLYGON_OID = 604,
    LINE_OID = 628,
    FLOAT4_OID = 700,
    FLOAT8_OID = 701,
    ABSTIME_OID = 702,
    RELTIME_OID = 703,
    TINTERVAL_OID = 704,
    UNKNOWN_OID = 705,
    CIRCLE_OID = 718,
    CASH_OID = 790,
    MACADDR_OID = 829,
    INET_OID = 869,
    CIDR_OID = 650,
    INT4ARRAY_OID = 1007,
    ACLITEM_OID = 1033,
    BPCHAR_OID = 1042,
    VARCHAR_OID = 1043,
    DATE_OID = 1082,
    TIME_OID = 1083,
    TIMESTAMP_OID = 1114,
    TIMESTAMPTZ_OID = 1184,
    INTERVAL_OID = 1186,
    TIMETZ_OID = 1266,
    BIT_OID = 1560,
    VARBIT_OID = 1562,
    NUMERIC_OID = 1700,
    REFCURSOR_OID = 1790,
    REGPROCEDURE_OID = 2202,
    REGOPER_OID = 2203,
    REGOPERATOR_OID = 2204,
    REGCLASS_OID = 2205,
    REGTYPE_OID = 2206,
    RECORD_OID = 2249,
    CSTRING_OID = 2275,
    ANY_OID = 2276,
    ANYARRAY_OID = 2277,
    VOID_OID = 2278,
    TRIGGER_OID = 2279,
    LANGUAGE_HANDLER_OID = 2280,
    INTERNAL_OID = 2281,
    OPAQUE_OID = 2282,
    ANYELEMENT_OID = 2283
} PGTYPE;

typedef enum {
    CONV_INT,
    CONV_STRING,
    CONV_FLOAT,
    CONV_BINARY,
    CONV_DATE,
    CONV_DATETIME,
    CONV_BOOL
} CONV;

typedef struct {
    PGresult *r;
    int nfields;
    int nrows;
    CONV *fields_convs;
    field *fields_ids;
    int current;
    value conv_date;
} result;


extern "C" {


void np_free_connection( value o )
{
    PQfinish( PGCONN( o )->m );
}

void np_free_result( value o )
{
    result *r = PGRESULT( o );
    PQclear( r->r );
}

static value np_reset_connection( value o )
{
    PQreset( PGCONN( o )->m );
}

static value np_connect( value params )
{
    PGconn *cnx;

    val_check( params, string );

    gc_enter_blocking();
    cnx = PQconnectdb( val_string( params ) );
    gc_exit_blocking();

    if (PQstatus(cnx) != CONNECTION_OK)
    {
        buffer b = alloc_buffer( "Connection to database failed: " );
        buffer_append( b, PQerrorMessage( cnx ) );
        PQfinish( cnx );
        bfailure( b );
    }

    value v = create_abstract( k_connection, sizeof(connection), np_free_connection);
    connection *c = PGCONN(v);
    c->m = cnx;
    return v;
}

static value np_result_next( value o )
{
    result *r;
    unsigned long *lengths = NULL;
    val_check_kind(o,k_result);
    r = PGRESULT(o);
    if( r->current >= (r->nrows) )
        return val_null;
    {
        int i;
        value cur = alloc_empty_object();
        if ( r->current == NULL ) r->current = 0;
        for(i=0;i<r->nfields;i++)
        {
            value v;
            switch( r->fields_convs[i] ) {
            case CONV_INT:
                v = alloc_best_int(atoi(PQgetvalue(r->r,r->current,i)));
                break;
            case CONV_STRING:
                v = alloc_string(PQgetvalue(r->r,r->current,i));
                break;
            case CONV_BOOL:
                v = alloc_bool( *PQgetvalue(r->r,r->current,i) != 'f' );
                break;
            case CONV_FLOAT:
                v = alloc_float(atof(PQgetvalue(r->r,r->current,i)));
                break;
            case CONV_BINARY:
                v = copy_string(PQgetvalue(r->r,r->current,i),PQgetlength(r->r,r->current,i));
                break;
            case CONV_DATE:
                if( r->conv_date == NULL )
                    v = alloc_string(PQgetvalue(r->r,r->current,i));
                else {
                    struct tm t;
                    sscanf(PQgetvalue(r->r,r->current,i),"%4d-%2d-%2d",&t.tm_year,&t.tm_mon,&t.tm_mday);
                    t.tm_hour = 0;
                    t.tm_min = 0;
                    t.tm_sec = 0;
                    t.tm_isdst = -1;
                    t.tm_year -= 1900;
                    t.tm_mon--;
                    v = val_call1(r->conv_date,alloc_int32((int)mktime(&t)));
                }
                break;
            case CONV_DATETIME:
                if( r->conv_date == NULL )
                    v = alloc_string(PQgetvalue(r->r,r->current,i));
                else {
                    struct tm t;
                    sscanf(PQgetvalue(r->r,r->current,i),"%4d-%2d-%2d %2d:%2d:%2d",&t.tm_year,&t.tm_mon,&t.tm_mday,&t.tm_hour,&t.tm_min,&t.tm_sec);
                    t.tm_isdst = -1;
                    t.tm_year -= 1900;
                    t.tm_mon--;
                    v = val_call1(r->conv_date,alloc_int32((int)mktime(&t)));
                }
                break;
            default:
                v = val_null;
                break;
            }
            alloc_field(cur,r->fields_ids[i],v);
        }
        r->current++;
        return cur;
    }
}

/**
    result_get : 'result -> n:int -> string
    <doc>Return the [n]th field of the current row</doc>
**/
static value np_result_get( value o, value n ) {
    result *r;
    const char *s;
    val_check_kind(o,k_result);
    val_check(n,int);
    r = PGRESULT(o);
    if( val_int(n) < 0 || val_int(n) >= r->nfields )
        neko_error();
    if( r->current == NULL )
        r->current++;
    s = PQgetvalue(r->r,r->current,val_int(n));
    return alloc_string( s?s:"" );
}

/**
    result_get_int : 'result -> n:int -> int
    <doc>Return the [n]th field of the current row as an integer (or 0)</doc>
**/
static value np_result_get_int( value o, value n ) {
    result *r;
    const char *s;
    val_check_kind(o,k_result);
    val_check(n,int);
    r = PGRESULT(o);
    if( val_int(n) < 0 || val_int(n) >= r->nfields )
        neko_error();
    if( r->current == NULL )
        r->current++;
    s = PQgetvalue(r->r,r->current,val_int(n));
    return alloc_int( s?atoi(s):0 );
}

/**
    result_get_float : 'result -> n:int -> float
    <doc>Return the [n]th field of the current row as a float (or 0)</doc>
**/
static value np_result_get_float( value o, value n ) {
    result *r;
    const char *s;
    val_check_kind(o,k_result);
    val_check(n,int);
    r = PGRESULT(o);
    if( val_int(n) < 0 || val_int(n) >= r->nfields )
        neko_error();
    if( r->current == NULL )
        r->current++;
    s = PQgetvalue(r->r,r->current,val_int(n));
    return alloc_float( s?atof(s):0 );
}

static CONV convert_type( PGTYPE t, unsigned int length ) {
    switch( t )
    {
        case BOOL_OID:
            //printf("bool\n");
            return CONV_BOOL;
        case BIT_OID:
        case VARBIT_OID:
            //printf("bool\n");
            if( length == 1 )
                return CONV_BOOL;
        case INT2_OID:
        case INT4_OID:
        case INT8_OID:
        case OID_OID:
        case TID_OID:
        case XID_OID:
        case CID_OID:
            //printf("int\n");
            return CONV_INT;
        case FLOAT4_OID:
        case FLOAT8_OID:
        case NUMERIC_OID:
        case CASH_OID:
            //printf("float\n");
            return CONV_FLOAT;
        case ABSTIME_OID:
        case RELTIME_OID:
        case TIME_OID:
        case TIMESTAMP_OID:
        case TIMESTAMPTZ_OID:
        case TIMETZ_OID:
            //printf("datetime\n");
            return CONV_DATETIME;
        case DATE_OID:
            //printf("date\n");
            return CONV_DATE;
        case BYTEA_OID:
        case CHAR_OID:
        case NAME_OID:
        case TEXT_OID:
        case BPCHAR_OID:
        case VARCHAR_OID:
        case CSTRING_OID:
            // I'm sure the above are string, but what about below?
        case INT2VECTOR_OID:
        case INT4ARRAY_OID:
        case OIDVECTOR_OID:
        case POINT_OID:
        case LSEG_OID:
        case PATH_OID:
        case BOX_OID:
        case POLYGON_OID:
        case LINE_OID:

        case REGPROC_OID:
        case PG_TYPE_RELTYPE_OID:
        case PG_ATTRIBUTE_RELTYPE_OID:
        case PG_PROC_RELTYPE_OID:
        case PG_CLASS_RELTYPE_OID:
        case TINTERVAL_OID:
        case UNKNOWN_OID:
        case CIRCLE_OID:
        case MACADDR_OID:
        case INET_OID:
        case CIDR_OID:

        case ACLITEM_OID:
        case INTERVAL_OID:
        case REFCURSOR_OID:
        case REGPROCEDURE_OID:
        case REGOPER_OID:
        case REGOPERATOR_OID:
        case REGCLASS_OID:
        case REGTYPE_OID:
        case RECORD_OID:
        case ANY_OID:
        case ANYARRAY_OID:
        case VOID_OID:
        case TRIGGER_OID:
        case LANGUAGE_HANDLER_OID:
        case INTERNAL_OID:
        case OPAQUE_OID:
        case ANYELEMENT_OID:
        /*
        case FIELD_TYPE_TINY:
            if( length == 1 )
                return CONV_BOOL;
        case FIELD_TYPE_SHORT:
        case FIELD_TYPE_LONG:
        case FIELD_TYPE_INT24:
            return CONV_INT;
        case FIELD_TYPE_LONGLONG:
        case FIELD_TYPE_DECIMAL:
        case FIELD_TYPE_FLOAT:
        case FIELD_TYPE_DOUBLE:
            return CONV_FLOAT;
        case FIELD_TYPE_BLOB:
            return CONV_BINARY;
        case FIELD_TYPE_DATETIME:
            return CONV_DATETIME;
        case FIELD_TYPE_DATE:
            return CONV_DATE;*/
        default:
            //printf("string\n");
            return CONV_STRING;
    }
}

/**
    result_set_conv_date : 'result -> function:1 -> void
    <doc>Set the function that will convert a Date or DateTime string
    to the corresponding value.</doc>
**/
static value np_result_set_conv_date( value o, value c )
{
    result *res;
    val_check_function( c, 1 );
    if( val_is_int( o ) )
        return val_true;
    val_check_kind( o, k_result );
    res = PGRESULT( o );
    res->conv_date = c;
    return val_true;
}

static value alloc_result( PGresult *r )
{
    value o = create_abstract(k_result, sizeof(result), np_free_result);
    result *res = PGRESULT(o);
    int num_fields = PQnfields(r);
    int num_rows = PQntuples(r);
    int i,j;
    res->r = r;
    res->conv_date = NULL;
    res->current = NULL;
    res->nfields = num_fields;

    // UPDATE, INSERT, DELETE queries don't have any fields, skip this
    // because libgc 1.0.3 crashes after alloc_private(0) is called enough times
    if (num_fields > 0)
    {
        res->nrows = num_rows;
        res->fields_ids = (field*)alloc_private(sizeof(field)*num_fields);
        res->fields_convs = (CONV*)alloc_private(sizeof(CONV)*num_fields);
        for( i=0; i<num_fields; i++ )
        {
            field id = val_id( PQfname( r, i ) );
            for( j=0; j<i; j++ )
                if( res->fields_ids[j] == id ) {
                    buffer b = alloc_buffer("Error, same field ids for : ");
                    buffer_append( b, PQfname( r, i ) );
                    buffer_append( b, ":" );
                    val_buffer( b, alloc_int( i ) );
                    buffer_append( b, " and " );
                    buffer_append( b, PQfname( r, i ) );
                    buffer_append( b, ":" );
                    val_buffer( b, alloc_int( j ) );
                    buffer_append( b, "." );
                    bfailure( b );
                }
            res->fields_ids[i] = id;
            res->fields_convs[i] = convert_type( (PGTYPE)PQftype( r, i ) , PQfsize( r, i ) );
        }
    }
    else
    {
        res->nrows = (int)atoi(PQcmdTuples(r));
        res->fields_ids = NULL;
        res->fields_convs = NULL;
    }
    return o;
}

static value np_request( value o, value r )
{
    PGresult *result;
    connection *c;

    val_check_kind( o, k_connection );
    val_check( r, string );
    c = PGCONN( o );

    gc_enter_blocking();
    result = PQexec( c->m, val_string( r ) );
    gc_exit_blocking();
    if (PQresultStatus(result) != PGRES_COMMAND_OK &&
        PQresultStatus(result) != PGRES_TUPLES_OK)
      {
        printf( "Query failed: [%s] (Result status: %i)",
          PQerrorMessage( c->m ), PQresultStatus(result) );
//            PQclear(result);

        // connection failed, trying to restore it
        if (PQresultStatus(result) == PGRES_FATAL_ERROR)
          {
            PQreset( c->m );

            // trying to resend query
            PQclear(result);

            gc_enter_blocking();
            result = PQexec( c->m, val_string( r ) );
            gc_exit_blocking();
          }
      }

    return alloc_result( result );
}

/* return the number of rows for a given result object */
static value np_result_get_length( value m )
{
    result *r;

    val_check_kind( m, k_result );

    r = PGRESULT( m );

    return alloc_int( PQntuples( r->r ) );
}

/* return the number of columns for a given result object */
static value np_result_get_nfields( value m )
{
    result *r;

    val_check_kind( m, k_result );

    r = PGRESULT( m );

    return alloc_int( PQnfields( r->r ) );
}

static value np_last_insert_id( value m )
{
    Oid id;
    result *res;

    val_check_kind( m, k_result );

    res = PGRESULT( m );

    id = PQoidValue( res->r );
    return alloc_int( id );
}

static value np_result_get_column_name( value m, value c )
{
    result *r;

    val_check_kind( m, k_result );
    val_check( c, int );

    r = PGRESULT( m );

    return alloc_string( PQfname( r->r, val_int( c ) ) );
}
/*
static value np_result_get_column_number( value m, value c )
{
    result *r;

    val_check_kind( m, k_result );
    val_check( c, string );

    r = PGRESULT( m );

    return alloc_string( PQfnumber( r->r, val_string( c ) ) );
}
*/
static value np_result_get_error( value m )
{
    result *r;

    val_check_kind( m, k_result );

    r = PGRESULT( m );

    return alloc_string( PQresultErrorMessage( r->r ) );
}


static value np_set_non_blocking( value m, value n ) {
    val_check_kind( m, k_connection );

    int ret = PQsetnonblocking( PGCONN(m)->m, val_int(n) );

    return alloc_int( ret );
}


static value np_is_non_blocking( value m ) {
    val_check_kind( m, k_connection );

    int ret = PQisnonblocking( PGCONN(m)->m );

    return alloc_bool( ret == 1 );
}


static value np_get_socket( value m, value sock ) {
    // hack: get proper k_socket "kind" from dummy socket
    vkind k_socket = val_kind(sock);
    int ret;

    val_check_kind( m, k_connection );

    ret = PQsocket( PGCONN(m)->m );

    return alloc_abstract(k_socket,(value)(int_val)ret);
}


static value np_flush( value m ) {
    val_check_kind( m, k_connection );

    int ret = PQflush( PGCONN(m)->m );

    return alloc_int( ret );
}


static value np_send_query(value m, value r)
{
    int result;
    value v;
    PGconn *conn;

    val_check_kind(m, k_connection);
    val_check(r, string);

    conn = PGCONN(m)->m;
    result = PQsendQuery(conn, val_string(r));
    if (result == 0)
      {
        printf("Query failed: [%s]", PQerrorMessage(conn));

        // connection failed, trying to restore it
        if (PQstatus(conn) == CONNECTION_BAD)
          {
            PQreset(conn);

            // trying to resend query
            result = PQsendQuery(conn, val_string(r));
          }
      }

    return alloc_bool(result == 1);
}


static value np_get_result( value m )
{
    PGconn *conn;
    PGresult *result;
    value v;

    val_check_kind( m, k_connection );

    conn = PGCONN(m)->m;
    result = PQgetResult( conn );
    if (result == NULL)
      return val_null;

    if (PQresultStatus(result) != PGRES_COMMAND_OK &&
        PQresultStatus(result) != PGRES_TUPLES_OK)
      {
        printf( "Query failed: [%s] (Result status:%i)",
          PQerrorMessage(conn),
          PQresultStatus(result) );
//            PQclear(result);

        // connection failed, trying to restore it
        if (PQresultStatus(result) == PGRES_FATAL_ERROR)
          {
            PQreset(conn);
            PQclear(result);

            return val_null;
          }
      }

    return alloc_result( result );
}


static value np_consume_input( value m ) {
    val_check_kind( m, k_connection );

    int ret = PQconsumeInput( PGCONN(m)->m );

    return alloc_bool( ret == 1 );
}


static value np_is_busy( value m ) {
    val_check_kind( m, k_connection );

    int ret = PQisBusy( PGCONN(m)->m );

    return alloc_bool( ret == 1 );
}


static value np_error_message( value m ) {
    val_check_kind( m, k_connection );

    return alloc_string(PQerrorMessage(PGCONN(m)->m));
}

} // extern "C"


DEFINE_PRIM(np_connect,1);
DEFINE_PRIM(np_free_connection,1);
DEFINE_PRIM(np_free_result,1);
DEFINE_PRIM(np_last_insert_id,1);
DEFINE_PRIM(np_request,2);

DEFINE_PRIM(np_set_non_blocking,2);
DEFINE_PRIM(np_is_non_blocking,1);
DEFINE_PRIM(np_get_socket,2);
DEFINE_PRIM(np_flush,1);
DEFINE_PRIM(np_send_query,2);
DEFINE_PRIM(np_get_result,1);
DEFINE_PRIM(np_consume_input,1);
DEFINE_PRIM(np_is_busy,1);
DEFINE_PRIM(np_error_message,1);

DEFINE_PRIM(np_reset_connection,1);
DEFINE_PRIM(np_result_get_column_name,2);
//DEFINE_PRIM(np_result_get_column_number,2);
DEFINE_PRIM(np_result_get_length,1);
DEFINE_PRIM(np_result_get_nfields,1);
DEFINE_PRIM(np_result_next,1);
DEFINE_PRIM(np_result_get,2);
DEFINE_PRIM(np_result_get_int,2);
DEFINE_PRIM(np_result_get_float,2);
DEFINE_PRIM(np_result_set_conv_date,2);
DEFINE_PRIM(np_result_get_error,1);
