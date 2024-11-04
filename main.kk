extern import
  c { syslib = "duckdb"}

extern import 
  c file "ddb"

value struct database
  inner : intptr_t

value struct connection
  inner : intptr_t
  
value struct result
  inner : intptr_t

value struct data-chunk
  inner : intptr_t

value struct vector
  inner : intptr_t

fun data-chunk/is-done( chunk : data-chunk ) : bool
  chunk.inner.is-null()

extern is-null( opaque : intptr_t ) : bool
  c inline "#1 == (intptr_t)NULL"

extern duckdb-version() : string
  c "kk_duckdb_version"
  
extern duckdb-open() : database
  c "kk_duckdb_open"

extern duckdb-connect( database : database ) : connection
  c "kk_duckdb_connect"

extern duckdb-close( database : database ) : ()
  c "kk_duckdb_close"

extern duckdb-disconnect( connection : connection ) : ()
  c "kk_duckdb_disconnect"

extern duckdb-query( connection : connection, command : string ) : result
  c "kk_duckdb_query"

extern duckdb-rows-changed( result : result ) : int
  c "kk_duckdb_rows_changed"

extern duckdb-column-name( result : result, idx : int ) : string
  c "kk_duckdb_column_name"

extern duckdb-column-type( result : result, idx : int ) : int
  c "kk_duckdb_column_type"

extern duckdb-column-count( result : result ) : int
  c "kk_duckdb_column_count"

extern duckdb-result-statement-type( result : result ) : int
  c "kk_duckdb_result_statement_type"

extern duckdb-result-error( result : result ) : string
  c "kk_duckdb_result_error"

extern duckdb-result-error-type( result : result ) : int
  c "kk_duckdb_result_error_type"
  
extern duckdb-destroy-result( result : result ) : ()
  c "kk_duckdb_destroy_result"

extern duckdb-fetch-chunk( result : result ) : data-chunk
  c "kk_duckdb_fetch_chunk"

extern duckdb-destroy-data-chunk( chunk : data-chunk ) : ()
  c "kk_duckdb_destroy_data_chunk" 

extern duckdb-data-chunk-get-size( chunk : data-chunk ) : int
  c "kk_duckdb_data_chunk_get_size"

extern duckdb-data-chunk-get-column-count( chunk : data-chunk ) : int
  c "kk_duckdb_data_chunk_get_column_count"

extern duckdb-data-chunk-get-vector( chunk : data-chunk, index : int ) : vector
  c "kk_duckdb_data_chunk_get_vector"

pub fun main() 
  val db = duckdb-open()
  val conn = db.duckdb-connect()
  val r1 = conn.duckdb-query("CREATE TABLE integers (i INTEGER, j INTEGER)")
  r1.duckdb-destroy-result()
  val r2 = conn.duckdb-query("INSERT INTO integers VALUES (3, 4), (5, 6), (7, NULL)")
  r2.duckdb-destroy-result()

  val res = conn.duckdb-query("SELECT * FROM integers")

  var chunk := res.duckdb-fetch-chunk()
  var count := chunk.duckdb-data-chunk-get-size()

  while { !chunk.is-done() }
    while { count != 0 }      
      println("Row")
      count := count - 1
    chunk.duckdb-destroy-data-chunk()
    chunk := res.duckdb-fetch-chunk()
    count := chunk.duckdb-data-chunk-get-size()

  res.duckdb-destroy-result()
  conn.duckdb-disconnect()
  db.duckdb-close()
