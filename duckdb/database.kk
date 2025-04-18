module duckdb/database

import duckdb/raw

import std/core/undiv
import std/core/vector

pub effect query-result
  fun read-int( ordinal : int ) : maybe<int>
  fun read-bool( ordinal : int ) : maybe<bool>
  fun read-string( ordinal : int ) : maybe<string>
  fun read-float64 ( ordinal : int ) : maybe<float64>

pub effect duckdb<e>
  fun query( sql : string, cb : () -> <query-result|e> () ) : ()
  fun execute( sql : string ) : int

value struct column
  type-id : int
  data : duckdb/raw/data

pub fun duckdb-open( action : () -> <duckdb<<exn|e>>,exn|e> () ) : <exn|e> ()
  val db = raw/duckdb-open()
  val conn = db.duckdb-connect()

  with finally
    conn.duckdb-disconnect()
    db.duckdb-close()

  with handler
    fun query(sql, result-action)
      val res = conn.duckdb-query(sql)

      with finally
        res.duckdb-destroy-result()

      with chunk <- traverse-chunks(res)
      val count = chunk.duckdb-data-chunk-get-size()
    
      for(count) fn(row)
        with handler
          fun read-int(ord) 
            with col <- column(chunk, ord, row)
            val data = col.data

            match col.type-id
              2 -> data.duckdb-data-read-tiny-int(row)
              3 -> data.duckdb-data-read-small-int(row)
              4 -> data.duckdb-data-read-integer(row)
              5 -> data.duckdb-data-read-big-int(row)
              6 -> data.duckdb-data-read-tiny-uint(row)
              7 -> data.duckdb-data-read-small-uint(row)
              8 -> data.duckdb-data-read-uinteger(row)
              9 -> data.duckdb-data-read-big-uint(row)
              _ -> throw("Column cannot be read as int")

          fun read-float64(ord)
            with col <- column(chunk, ord, row)
            
            if col.type-id != 11 
            then throw("Column cannot be read as float64")
            else col.data.duckdb-data-read-double(row)

          fun read-bool(ord)
            with col <- column(chunk, ord, row)

            if col.type-id != 1
            then throw("Column cannot be read as boolean")
            else col.data.duckdb-data-read-boolean(row)

          fun read-string(ord)
            with col <- column(chunk, ord, row)

            if col.type-id != 17 
            then throw("Column cannot be read as varchar")
            else col.data.duckdb-data-read-varchar(row)

        result-action()
        
    fun execute(sql)
      val result = conn.duckdb-query(sql)
      val changed = result.duckdb-rows-changed()
      result.duckdb-destroy-result()
      changed

  action()

fun column<a>( chunk : duckdb/raw/data-chunk, ord : int, row : int, action : (column) -> <exn|e> a ) : <exn|e> maybe<a>
  val col-count = chunk.duckdb-data-chunk-get-column-count()

  // First check that we are in range
  if col-count < 0 || ord >= col-count
  then throw("Column out of range " ++ ord.show)
  else 
    val col = chunk.duckdb-data-chunk-get-vector(ord)

    val validity = col.duckdb-vector-get-validity()
    val ty = col.duckdb-vector-get-column-type()

    // Check if the colum x row is null
    if validity.duckdb-validity-row-is-valid(row)
    then Just(action(Column(ty.duckdb-get-type-id(), col.duckdb-vector-get-data())))
    else Nothing
  
tail fun traverse-chunks( result : raw/result, action : (raw/data-chunk) -> e () ) : e ()
  val chunk = result.duckdb-fetch-chunk()  
  
  if !chunk.is-done then
    finally { chunk.duckdb-destroy-data-chunk() }
      action(chunk)    
    traverse-chunks( result.pretend-decreasing(), action )
