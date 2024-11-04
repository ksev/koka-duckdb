module duckdb/raw

extern import
  c { syslib = "duckdb"}

extern import 
  c file "inline/duckdb.h"

pub value struct database
  inner : intptr_t

pub value struct connection
  inner : intptr_t
  
pub value struct result
  inner : intptr_t

pub value struct data-chunk
  inner : intptr_t

pub value struct vector
  inner : intptr_t

pub value struct data
  inner : intptr_t

pub value struct validity
  inner : intptr_t

pub fun data-chunk/is-done( chunk : data-chunk ) : bool
  chunk.inner.is-null()

pub extern is-null( opaque : intptr_t ) : bool
  c inline "#1 == (intptr_t)NULL"

pub extern duckdb-version() : string
  c "kk_duckdb_version"
  
pub extern duckdb-open() : database
  c "kk_duckdb_open"

pub extern duckdb-connect( database : database ) : connection
  c "kk_duckdb_connect"

pub extern duckdb-close( database : database ) : ()
  c "kk_duckdb_close"

pub extern duckdb-disconnect( connection : connection ) : ()
  c "kk_duckdb_disconnect"

pub extern duckdb-query( connection : connection, command : string ) : result
  c "kk_duckdb_query"

pub extern duckdb-rows-changed( result : result ) : int
  c "kk_duckdb_rows_changed"

pub extern duckdb-column-name( result : result, idx : int ) : string
  c "kk_duckdb_column_name"

pub extern duckdb-column-type( result : result, idx : int ) : int
  c "kk_duckdb_column_type"

pub extern duckdb-column-count( result : result ) : int
  c "kk_duckdb_column_count"

pub extern duckdb-result-statement-type( result : result ) : int
  c "kk_duckdb_result_statement_type"

pub extern duckdb-result-error( result : result ) : string
  c "kk_duckdb_result_error"

pub extern duckdb-result-error-type( result : result ) : int
  c "kk_duckdb_result_error_type"
  
pub extern duckdb-destroy-result( result : result ) : ()
  c "kk_duckdb_destroy_result"

pub extern duckdb-fetch-chunk( result : result ) : data-chunk
  c "kk_duckdb_fetch_chunk"

pub extern duckdb-destroy-data-chunk( chunk : data-chunk ) : ()
  c "kk_duckdb_destroy_data_chunk" 

pub extern duckdb-data-chunk-get-size( chunk : data-chunk ) : int
  c "kk_duckdb_data_chunk_get_size"

pub extern duckdb-data-chunk-get-column-count( chunk : data-chunk ) : int
  c "kk_duckdb_data_chunk_get_column_count"

pub extern duckdb-data-chunk-get-vector( chunk : data-chunk, index : int ) : vector
  c "kk_duckdb_data_chunk_get_vector"

pub extern duckdb-vector-get-data( vector : vector ) : data
  c "kk_duckdb_vector_get_data"

pub extern duckdb-vector-get-validity( vector : vector ) : validity
  c "kk_duckdb_vector_get_validity"

pub extern duckdb-validity-row-is-valid( validity : validity, row : int ) : bool
  c "kk_duckdb_validity_row_is_valid"

pub extern duckdb-data-read-integer( data : data, row : int ) : int
  c "kk_duckdb_data_read_integer"

