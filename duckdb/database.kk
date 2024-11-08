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

struct chunk-state
  data : data
  validity : validity
  ctype : logical-type
  type-id : int

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
      val col-count = chunk.duckdb-data-chunk-get-column-count()

      val cols = vector-init(col-count) fn(i)
        val col = chunk.duckdb-data-chunk-get-vector(i)
        val col-data = col.duckdb-vector-get-data()
        val col-validity = col.duckdb-vector-get-validity()
        val col-type = col.duckdb-vector-get-column-type()
        val col-type-id = col-type.duckdb-get-type-id()
        Chunk-state(col-data, col-validity, col-type, col-type-id)
    
      for(count) fn(row)
        with handler
          fun read-int(ord) 
            with state <- valid-col(cols, ord, row)
            val data = state.data

            match state.type-id
              2 -> data.duckdb-data-read-tiny-int(row)
              3 -> data.duckdb-data-read-small-int(row)
              4 -> data.duckdb-data-read-integer(row)
              5 -> data.duckdb-data-read-big-int(row)
              6 -> data.duckdb-data-read-tiny-uint(row)
              7 -> data.duckdb-data-read-small-uint(row)
              8 -> data.duckdb-data-read-uinteger(row)
              9 -> data.duckdb-data-read-big-uint(row)
              _ -> throw("Column cannot be read as int")

          fun read-bool(ord)
            with state <- valid-col(cols, ord, row)
            val data = state.data

            if state.type-id != 1 
            then throw("Column cannot be read as boolean")
            else data.duckdb-data-read-boolean(row)

        result-action()
        
    fun execute(sql)
      val result = conn.duckdb-query(sql)
      val changed = result.duckdb-rows-changed()
      result.duckdb-destroy-result()
      changed

  action()

fun valid-col<a>( state : std/core/types/vector<chunk-state>, ord : int, row : int, action : (chunk-state) -> <exn|e> a ) : <exn|e> maybe<a>
  val st = state.at(ord).unjust
  if st.validity.duckdb-validity-row-is-valid(row) 
  then Just(action(st))
  else Nothing

tail fun traverse-chunks( result : raw/result, action : (raw/data-chunk) -> e () ) : e ()
  val chunk = result.duckdb-fetch-chunk()  
  
  if !chunk.is-done then
    finally { chunk.duckdb-destroy-data-chunk() }
      action(chunk)    
    traverse-chunks( result.pretend-decreasing(), action )
