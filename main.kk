import duckdb/database
import std/num/float64

pub fun main()
  with duckdb-open()

  execute("CREATE TABLE integers (i smallint, j varchar, k double)")
  execute("INSERT INTO integers VALUES (3, 'some text', 0.0), (5, 'hello', 2.4), (7, NULL, 1.0)")

  query("SELECT * FROM integers")
    val c1 = read-int(0)
    val c2 = read-string(1)
    val c3 = read-float64(2)
   
    println((c1, c2, c3.map fn(f) { f.show(1) }))
