import duckdb/database

pub fun main() 
  with duckdb-open()

  execute("CREATE TABLE integers (i INTEGER, j INTEGER)").println
  execute("INSERT INTO integers VALUES (3, 4), (5, 6), (7, NULL)").println

  query("SELECT * FROM integers")
    with handler
      fun read-int(ord) 55
    read-int(0).println
    read-int(1).println
    ()
  
/*
  val res = conn.duckdb-query("SELECT * FROM integers")

  var chunk := res.duckdb-fetch-chunk()
  var count := chunk.duckdb-data-chunk-get-size()
  var row := 0;

  while { !chunk.is-done() }
    val col1 = chunk.duckdb-data-chunk-get-vector(0)
    val col1-data = col1.duckdb-vector-get-data()
    val col1-validity = col1.duckdb-vector-get-validity()

    val col2 = chunk.duckdb-data-chunk-get-vector(1)
    val col2-data = col2.duckdb-vector-get-data()
    val col2-validity = col2.duckdb-vector-get-validity()

    while { row < count }      
      if col1-validity.duckdb-validity-row-is-valid(row) then col1-data.duckdb-data-read-integer(row).print
      else "NULL,".print

      if col2-validity.duckdb-validity-row-is-valid(row) then col2-data.duckdb-data-read-integer(row).print
      else "NULL,".print

      println("")

      row := row + 1
    chunk.duckdb-destroy-data-chunk()
    chunk := res.duckdb-fetch-chunk()
    count := chunk.duckdb-data-chunk-get-size()
    row := 0


*/