Toying around with a duckdb bindings for koka.

```koka
import duckdb/database

pub fun main() 
  with duckdb-open()

  execute("CREATE TABLE integers (i INTEGER, j INTEGER)")
  execute("INSERT INTO integers VALUES (3, 4), (5, 6), (7, NULL)")

  query("SELECT * FROM integers") 
    val c1 = read-int(0)
    val c2 = read-int(1)
    
    println((c1, c2))
```
