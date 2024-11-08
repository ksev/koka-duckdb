import duckdb/database

pub fun main() 
  with duckdb-open()

  execute("CREATE TABLE integers (i smallint, j varchar)")
  execute("INSERT INTO integers VALUES (3, 'detta är lite svenska'), (5, 'hello'), (7, NULL)")

  query("SELECT * FROM integers") 
    val c1 = read-int(0)
    val c2 = read-string(1)

    println((c1, c2))
