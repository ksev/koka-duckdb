#include <duckdb.h>
#include <stdio.h>

struct kk_string_s kk_duckdb_version(kk_context_t *ctx) {
  const char *str = duckdb_library_version();
  return kk_string_alloc_dup_valid_utf8(str, ctx);
}

kk_duckdb_raw__database kk_duckdb_open(kk_context_t *ctx) {
  duckdb_database db;

  if (duckdb_open(NULL, &db) == DuckDBError) {
    return kk_duckdb_raw__new_Database((intptr_t)NULL, ctx);
  }

  return kk_duckdb_raw__new_Database((intptr_t)db, ctx);
}

void kk_duckdb_close(kk_duckdb_raw__database database, kk_context_t *ctx) {
  duckdb_close((duckdb_database *)&database.inner);
}

kk_duckdb_raw__connection kk_duckdb_connect(kk_duckdb_raw__database db, kk_context_t *ctx) {
  duckdb_connection con;

  if (duckdb_connect((duckdb_database)db.inner, &con) == DuckDBError) {
    return kk_duckdb_raw__new_Connection((intptr_t)NULL, ctx);
  }

  return kk_duckdb_raw__new_Connection((intptr_t)con, ctx);
}

void kk_duckdb_disconnect(kk_duckdb_raw__connection connection, kk_context_t *ctx) {
  duckdb_disconnect((duckdb_connection *)&connection.inner);
}

kk_duckdb_raw__result kk_duckdb_query(kk_duckdb_raw__connection connection,
                                kk_string_t query, kk_context_t *ctx) {
  duckdb_state state;
  duckdb_result res;

  bool should_free;
  const char *query_c = kk_string_to_qutf8_borrow(query, &should_free, ctx);

  state = duckdb_query((duckdb_connection)connection.inner, query_c, &res);

  if (state == DuckDBError) {
    return kk_duckdb_raw__new_Result((intptr_t)NULL, ctx);
  }

  return kk_duckdb_raw__new_Result((intptr_t)res.internal_data, ctx);
}

kk_string_t kk_duckdb_column_name(kk_duckdb_raw__result result, kk_integer_t idx,
                                  kk_context_t *ctx) {
  duckdb_result res;
  res.internal_data = (void *)result.inner;

  idx_t col = (idx_t)kk_integer_clamp64_generic(idx, ctx);
  const char *name = duckdb_column_name(&res, col);

  return kk_string_alloc_dup_valid_utf8(name, ctx);
}

kk_integer_t kk_duckdb_column_type(kk_duckdb_raw__result result, kk_integer_t idx,
                                   kk_context_t *ctx) {
  duckdb_result res;
  res.internal_data = (void *)result.inner;

  idx_t col = (idx_t)kk_integer_clamp64_generic(idx, ctx);
  int status = duckdb_column_type(&res, col);

  return kk_integer_from_int(status, ctx);
}

kk_integer_t kk_duckdb_result_statement_type(kk_duckdb_raw__result result,
                                             kk_context_t *ctx) {
  duckdb_result res;
  res.internal_data = (void *)result.inner;

  int type = duckdb_result_statement_type(res);

  return kk_integer_from_int(type, ctx);
}

kk_integer_t kk_duckdb_column_count(kk_duckdb_raw__result result, kk_context_t *ctx) {
  duckdb_result res;
  res.internal_data = (void *)result.inner;

  int count = duckdb_column_count(&res);

  return kk_integer_from_bigu64(count, ctx);
}

kk_integer_t kk_duckdb_rows_changed(kk_duckdb_raw__result result, kk_context_t *ctx) {
  duckdb_result res;
  res.internal_data = (void *)result.inner;

  idx_t count = duckdb_rows_changed(&res);

  if (count == 0)
  {
    return kk_integer_zero;
  }

  return kk_integer_from_bigu64(count, ctx);
}

kk_string_t kk_duckdb_result_error(kk_duckdb_raw__result result, kk_context_t *ctx) {
  duckdb_result res;
  res.internal_data = (void *)result.inner;

  const char *name = duckdb_result_error(&res);

  return kk_string_alloc_dup_valid_utf8(name, ctx);
}

kk_integer_t kk_duckdb_result_error_type(kk_duckdb_raw__result result,
                                         kk_context_t *ctx) {
  duckdb_result res;
  res.internal_data = (void *)result.inner;

  int type = duckdb_result_error_type(&res);

  return kk_integer_from_int(type, ctx);
}

void kk_duckdb_destroy_result(kk_duckdb_raw__result result, kk_context_t *ctx) {
  duckdb_result res;
  res.internal_data = (void *)result.inner;
  res.deprecated_columns = (void *)NULL;
  res.deprecated_error_message = (void *)NULL;

  duckdb_destroy_result(&res);
}

kk_duckdb_raw__data_chunk kk_duckdb_fetch_chunk(kk_duckdb_raw__result result,
                                          kk_context_t *ctx) {
  duckdb_result res;
  res.internal_data = (void *)result.inner;

  duckdb_data_chunk chunk = duckdb_fetch_chunk(res);
  return kk_duckdb_raw__new_Data_chunk((intptr_t)chunk, ctx);
}

kk_integer_t kk_duckdb_data_chunk_get_size(kk_duckdb_raw__data_chunk chunk,
                                           kk_context_t *ctx) {
  idx_t size = duckdb_data_chunk_get_size((duckdb_data_chunk)chunk.inner);
  return kk_integer_from_bigu64(size, ctx);
}

kk_integer_t kk_duckdb_data_chunk_get_column_count(kk_duckdb_raw__data_chunk chunk,
                                                   kk_context_t *ctx) {
  idx_t size =
      duckdb_data_chunk_get_column_count((duckdb_data_chunk)chunk.inner);
  return kk_integer_from_bigu64(size, ctx);
}

kk_duckdb_raw__vector kk_duckdb_data_chunk_get_vector(kk_duckdb_raw__data_chunk chunk, kk_integer_t idx, kk_context_t *ctx) {
  idx_t col = (idx_t)kk_integer_clamp64_generic(idx, ctx);

  duckdb_vector vector = duckdb_data_chunk_get_vector((duckdb_data_chunk)chunk.inner, col);

  return kk_duckdb_raw__new_Vector((intptr_t)vector, ctx);
}

void kk_duckdb_destroy_data_chunk(kk_duckdb_raw__data_chunk chunk,
                                  kk_context_t *ctx) {
  duckdb_destroy_data_chunk((duckdb_data_chunk *)&chunk.inner);
}

kk_duckdb_raw__data kk_duckdb_vector_get_data(kk_duckdb_raw__vector vector, kk_context_t *ctx) {
  intptr_t data = (intptr_t)duckdb_vector_get_data((duckdb_vector)vector.inner);

  return kk_duckdb_raw__new_Data(data, ctx);
}

kk_duckdb_raw__validity kk_duckdb_vector_get_validity(kk_duckdb_raw__vector val, kk_context_t *ctx) {
  intptr_t data = (intptr_t)duckdb_vector_get_validity((duckdb_vector)val.inner);
  return kk_duckdb_raw__new_Validity(data, ctx);
}

bool kk_duckdb_validity_row_is_valid(kk_duckdb_raw__validity val, kk_integer_t idx, kk_context_t *ctx) {
  idx_t col = (idx_t)kk_integer_clamp64_generic(idx, ctx);
  return duckdb_validity_row_is_valid((uint64_t*)val.inner, col);
}

kk_integer_t kk_duckdb_data_read_integer(kk_duckdb_raw__data data, kk_integer_t idx, kk_context_t *ctx) {
  idx_t col = (idx_t)kk_integer_clamp64_generic(idx, ctx);

  int32_t value = ((int32_t*)data.inner)[col];
  
  return kk_integer_from_big64((int64_t)value, ctx);
}
