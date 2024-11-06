module duckdb/database

import duckdb/raw
import std/core/undiv
import std/core/vector

pub effect query-result
  fun read-int( ordinal : int ) : maybe<int>
  fun read-bool( ordinal : int ) : maybe<bool>

pub effect duckdb<e>
  fun query( sql : string, cb : () -> <query-result|e> () ) : ()
  fun execute( sql : string ) : int

pub fun duckdb-open( action : () -> <duckdb<<exn|e>>,exn|e> () ) : <exn|e> ()
  val db = raw/duckdb-open()
  val conn = db.duckdb-connect()

  with handler
    fun query(sql, cb)
      val res = conn.duckdb-query(sql)
      with finally { res.duckdb-destroy-result }

      with chunk <- traverse-chunks(res)
      val count = chunk.duckdb-data-chunk-get-size()
      val col-count = chunk.duckdb-data-chunk-get-column-count()

      val cols = vector-init(col-count) fn(i)
        val col = chunk.duckdb-data-chunk-get-vector(i)
        val col-data = col.duckdb-vector-get-data()
        val col-validity = col.duckdb-vector-get-validity()
        (col, col-data, col-validity)
    
      for(count) fn(row)
        with handler
          fun read-int(ord) 
            val (_, data, valid) = cols.at(ord).unjust
            if valid.duckdb-validity-row-is-valid(row) 
            then Just(data.duckdb-data-read-integer(row))
            else Nothing

          fun read-bool(ord) Just(False)
        cb()
        
    fun execute(sql)
      val result = conn.duckdb-query(sql)
      val changed = result.duckdb-rows-changed()
      result.duckdb-destroy-result()
      changed

    return(_) 
      conn.duckdb-disconnect()
      db.duckdb-close()

  action()

/*
pub fun build-vector( action : () -> <query-result|e> () ) : e ()
  with handler
    fun read-int(ord) 55
  action()
  */
fun traverse-chunks( result : raw/result, cb : (raw/data-chunk) -> e () ) : e ()
  val chunk = result.duckdb-fetch-chunk()  
  if chunk.is-done then ()
  else 
    cb(chunk)
    chunk.duckdb-destroy-data-chunk()   
    traverse-chunks( result.pretend-decreasing(), cb )
