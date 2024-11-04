#include "ddb.h"
#include "duckdb.h"
#include <stdint.h>
#include <stdio.h>

struct kk_string_s kk_duckdb_version(kk_context_t *ctx) {
  const char *str = duckdb_library_version();
  return kk_string_alloc_dup_valid_utf8(str, ctx);
}

kk_main__database kk_duckdb_open(kk_context_t *ctx) {
  duckdb_database db;

  if (duckdb_open(NULL, &db) == DuckDBError) {
    return kk_main__new_Database((intptr_t)NULL, ctx);
  }

  return kk_main__new_Database((intptr_t)db, ctx);
}

void kk_duckdb_close(kk_main__database database, kk_context_t *ctx) {
  duckdb_close((duckdb_database *)&database.inner);
}

kk_main__connection kk_duckdb_connect(kk_main__database db, kk_context_t *ctx) {
  duckdb_connection con;

  if (duckdb_connect((duckdb_database)db.inner, &con) == DuckDBError) {
    return kk_main__new_Connection((intptr_t)NULL, ctx);
  }

  return kk_main__new_Connection((intptr_t)con, ctx);
}

void kk_duckdb_disconnect(kk_main__connection connection, kk_context_t *ctx) {
  duckdb_disconnect((duckdb_connection *)&connection.inner);
}

kk_main__result kk_duckdb_query(kk_main__connection connection,
                                kk_string_t query, kk_context_t *ctx) {
  duckdb_state state;
  duckdb_result res;

  bool should_free;
  const char *query_c = kk_string_to_qutf8_borrow(query, &should_free, ctx);

  state = duckdb_query((duckdb_connection)connection.inner, query_c, &res);

  if (state == DuckDBError) {
    return kk_main__new_Result((intptr_t)NULL, ctx);
  }

  return kk_main__new_Result((intptr_t)res.internal_data, ctx);
}

kk_string_t kk_duckdb_column_name(kk_main__result result, kk_integer_t idx,
                                  kk_context_t *ctx) {
  duckdb_result res;
  res.internal_data = (void *)result.inner;

  idx_t col = (idx_t)kk_integer_clamp64_generic(idx, ctx);
  const char *name = duckdb_column_name(&res, col);

  return kk_string_alloc_dup_valid_utf8(name, ctx);
}

kk_integer_t kk_duckdb_column_type(kk_main__result result, kk_integer_t idx,
                                   kk_context_t *ctx) {
  duckdb_result res;
  res.internal_data = (void *)result.inner;

  idx_t col = (idx_t)kk_integer_clamp64_generic(idx, ctx);
  int status = duckdb_column_type(&res, col);

  return kk_integer_from_int(status, ctx);
}

kk_integer_t kk_duckdb_result_statement_type(kk_main__result result,
                                             kk_context_t *ctx) {
  duckdb_result res;
  res.internal_data = (void *)result.inner;

  int type = duckdb_result_statement_type(res);

  return kk_integer_from_int(type, ctx);
}

kk_integer_t kk_duckdb_column_count(kk_main__result result, kk_context_t *ctx) {
  duckdb_result res;
  res.internal_data = (void *)result.inner;

  int count = duckdb_column_count(&res);

  return kk_integer_from_bigu64(count, ctx);
}

kk_integer_t kk_duckdb_rows_changed(kk_main__result result, kk_context_t *ctx) {
  duckdb_result res;
  res.internal_data = (void *)result.inner;

  idx_t count = duckdb_rows_changed(&res);

  return kk_integer_from_bigu64(count, ctx);
}

kk_string_t kk_duckdb_result_error(kk_main__result result, kk_context_t *ctx) {
  duckdb_result res;
  res.internal_data = (void *)result.inner;

  const char *name = duckdb_result_error(&res);

  return kk_string_alloc_dup_valid_utf8(name, ctx);
}

kk_integer_t kk_duckdb_result_error_type(kk_main__result result,
                                         kk_context_t *ctx) {
  duckdb_result res;
  res.internal_data = (void *)result.inner;

  int type = duckdb_result_error_type(&res);

  return kk_integer_from_int(type, ctx);
}

void kk_duckdb_destroy_result(kk_main__result result, kk_context_t *ctx) {
  duckdb_result res;
  res.internal_data = (void *)result.inner;
  res.deprecated_columns = (void *)NULL;
  res.deprecated_error_message = (void *)NULL;

  duckdb_destroy_result(&res);
}

kk_main__data_chunk kk_duckdb_fetch_chunk(kk_main__result result,
                                          kk_context_t *ctx) {
  duckdb_result res;
  res.internal_data = (void *)result.inner;

  duckdb_data_chunk chunk = duckdb_fetch_chunk(res);
  return kk_main__new_Data_chunk((intptr_t)chunk, ctx);
}

kk_integer_t kk_duckdb_data_chunk_get_size(kk_main__data_chunk chunk,
                                           kk_context_t *ctx) {
  idx_t size = duckdb_data_chunk_get_size((duckdb_data_chunk)chunk.inner);
  return kk_integer_from_bigu64(size, ctx);
}

kk_integer_t kk_duckdb_data_chunk_get_column_count(kk_main__data_chunk chunk,
                                                   kk_context_t *ctx) {
  idx_t size =
      duckdb_data_chunk_get_column_count((duckdb_data_chunk)chunk.inner);
  return kk_integer_from_bigu64(size, ctx);
}

kk_main__vector kk_duckdb_data_chunk_get_vector(kk_main__data_chunk chunk, kk_integer_t idx, kk_context_t *ctx) {
  idx_t col = (idx_t)kk_integer_clamp64_generic(idx, ctx);

  duckdb_vector vector = duckdb_data_chunk_get_vector((duckdb_data_chunk)chunk.inner, col);

  return kk_main__new_Vector((intptr_t)vector, ctx);
}

void kk_duckdb_destroy_data_chunk(kk_main__data_chunk chunk,
                                  kk_context_t *ctx) {
  duckdb_destroy_data_chunk((duckdb_data_chunk *)&chunk.inner);
}
