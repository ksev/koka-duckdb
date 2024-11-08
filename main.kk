import duckdb/database

pub fun main() 
  with duckdb-open()

  execute("CREATE TABLE integers (i smallint, j integer, k boolean, x bigint)")
  execute("INSERT INTO integers VALUES (3, 4, false, 5000), (5, 6, true, NULL), (7, NULL, NULL, NULL)")

  query("SELECT * FROM integers") 
    val c1 = read-int(0)
    val c2 = read-int(1)
    val c3 = read-bool(2)
    val c4 = read-int(3)

    println(c4)
