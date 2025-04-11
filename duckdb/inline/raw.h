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

  kk_with_string_as_qutf8_borrow(query, query_c, ctx) {
    state = duckdb_query((duckdb_connection)connection.inner, query_c, &res);
  }

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

/**
 * READ COLUMNS BLOCK
 */

bool kk_duckdb_data_read_boolean(kk_duckdb_raw__data data, kk_integer_t idx, kk_context_t *ctx) {
  idx_t col = (idx_t)kk_integer_clamp64_generic(idx, ctx);

  return ((bool*)data.inner)[col];
}

kk_integer_t kk_duckdb_data_read_tiny_int(kk_duckdb_raw__data data, kk_integer_t idx, kk_context_t *ctx) {
  idx_t col = (idx_t)kk_integer_clamp64_generic(idx, ctx);

  int8_t value = ((int8_t*)data.inner)[col];
  
  return kk_integer_from_big64((int64_t)value, ctx);
}

kk_integer_t kk_duckdb_data_read_small_int(kk_duckdb_raw__data data, kk_integer_t idx, kk_context_t *ctx) {
  idx_t col = (idx_t)kk_integer_clamp64_generic(idx, ctx);

  int16_t value = ((int16_t*)data.inner)[col];
  
  return kk_integer_from_big64((int64_t)value, ctx);
}

kk_integer_t kk_duckdb_data_read_integer(kk_duckdb_raw__data data, kk_integer_t idx, kk_context_t *ctx) {
  idx_t col = (idx_t)kk_integer_clamp64_generic(idx, ctx);

  int32_t value = ((int32_t*)data.inner)[col];
  
  return kk_integer_from_big64((int64_t)value, ctx);
}

kk_integer_t kk_duckdb_data_read_big_int(kk_duckdb_raw__data data, kk_integer_t idx, kk_context_t *ctx) {
  idx_t col = (idx_t)kk_integer_clamp64_generic(idx, ctx);

  int64_t value = ((int64_t*)data.inner)[col];
  
  return kk_integer_from_big64(value, ctx);
}

kk_integer_t kk_duckdb_data_read_tiny_uint(kk_duckdb_raw__data data, kk_integer_t idx, kk_context_t *ctx) {
  idx_t col = (idx_t)kk_integer_clamp64_generic(idx, ctx);

  uint8_t value = ((uint8_t*)data.inner)[col];
  
  return kk_integer_from_bigu64((uint64_t)value, ctx);
}

kk_integer_t kk_duckdb_data_read_small_uint(kk_duckdb_raw__data data, kk_integer_t idx, kk_context_t *ctx) {
  idx_t col = (idx_t)kk_integer_clamp64_generic(idx, ctx);

  uint16_t value = ((uint16_t*)data.inner)[col];
  
  return kk_integer_from_bigu64((uint64_t)value, ctx);
}

kk_integer_t kk_duckdb_data_read_uinteger(kk_duckdb_raw__data data, kk_integer_t idx, kk_context_t *ctx) {
  idx_t col = (idx_t)kk_integer_clamp64_generic(idx, ctx);

  uint32_t value = ((uint32_t*)data.inner)[col];
  
  return kk_integer_from_bigu64((uint64_t)value, ctx);
}

kk_integer_t kk_duckdb_data_read_big_uint(kk_duckdb_raw__data data, kk_integer_t idx, kk_context_t *ctx) {
  idx_t col = (idx_t)kk_integer_clamp64_generic(idx, ctx);

  uint64_t value = ((uint64_t*)data.inner)[col];
  
  return kk_integer_from_bigu64(value, ctx);
}

kk_string_t kk_duckdb_data_read_varchar(kk_duckdb_raw__data data, kk_integer_t idx, kk_context_t *ctx) {
  idx_t col = (idx_t)kk_integer_clamp64_generic(idx, ctx);

  duckdb_string_t str = ((duckdb_string_t*)data.inner)[col];

  if (duckdb_string_is_inlined(str)) {
    return kk_string_alloc_from_qutf8n(str.value.inlined.length, str.value.inlined.inlined, ctx);
  } 

  return kk_string_alloc_from_qutf8n(str.value.pointer.length, str.value.pointer.ptr,  ctx);
}

double kk_duckdb_data_read_double(kk_duckdb_raw__data data, kk_integer_t idx, kk_context_t *ctx) {
  idx_t col = (idx_t)kk_integer_clamp64_generic(idx, ctx);

  double value = ((double*)data.inner)[col];
  
  return value;
}

/**
 * Vector metadata and types
 */

kk_duckdb_raw__logical_type kk_duckdb_vector_get_column_type(kk_duckdb_raw__vector val, kk_context_t *ctx) {
  intptr_t ty = (intptr_t)duckdb_vector_get_column_type((duckdb_vector)val.inner);
  return kk_duckdb_raw__new_Logical_type(ty, ctx);
}

kk_integer_t kk_duckdb_get_type_id(kk_duckdb_raw__logical_type type, kk_context_t *ctx) {
  int64_t ty = (int64_t)duckdb_get_type_id((duckdb_logical_type)type.inner);
  return kk_integer_from_big64((int64_t)ty, ctx);
}
