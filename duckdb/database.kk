module duckdb/database

import duckdb/raw
import std/core/undiv

pub effect result
  fun read-int( ordinal : int ) : int

pub effect duckdb
  fun query( sql : string, cb : () -> e () ) : ()
  fun execute( sql : string ) : int

pub fun duckdb-open( action : () -> <duckdb|e> () ) : e ()
  val db = raw/duckdb-open()
  val conn = db.duckdb-connect()

  with handler
    fun query(sql, cb)
      val result = conn.duckdb-query(sql)
      
      result.traverse-chunks fn(chunk)
        ()

    fun execute(sql)
      val result = conn.duckdb-query(sql)
      val changed = result.duckdb-rows-changed()
      result.duckdb-destroy-result()
      changed

    return(_) 
      conn.duckdb-disconnect()
      db.duckdb-close()

  action()

pub fun build-vector( action : () -> <result|e> () ) : e ()
  with handler
    fun read-int(ord) 55
  action()
  
fun traverse-chunks( result : raw/result, cb : (raw/data-chunk) -> e () ) : e ()
  val chunk = result.duckdb-fetch-chunk()
  if chunk.is-done then ()
  else 
    cb(chunk)
    traverse-chunks( result.pretend-decreasing(), cb )
