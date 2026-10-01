// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <cstdio>
#include <cstdlib>
#include <duckdb.h>
#include <string>
#include <vector>

namespace {

auto require(bool condition, char const* message) -> void {
  if (not condition) {
    std::fprintf(stderr, "FAIL: %s\n", message ? message : "unknown error");
    std::exit(EXIT_FAILURE);
  }
}

struct Result {
  duckdb_result value{};

  ~Result() {
    duckdb_destroy_result(&value);
  }
};

auto query(duckdb_connection con, std::string const& sql) -> void {
  auto result = Result{};
  if (duckdb_query(con, sql.c_str(), &result.value) != DuckDBSuccess) {
    std::fprintf(stderr, "SQL: %s\n", sql.c_str());
    require(false, duckdb_result_error(&result.value));
  }
}

auto scalar(duckdb_connection con, std::string const& sql) -> int64_t {
  auto result = Result{};
  auto status = duckdb_query(con, sql.c_str(), &result.value);
  require(status == DuckDBSuccess, duckdb_result_error(&result.value));
  require(duckdb_row_count(&result.value) == 1, "expected one row");
  return duckdb_value_int64(&result.value, 0, 0);
}

struct Database {
  duckdb_database db{};
  duckdb_connection con{};

  Database() {
    auto config = duckdb_config{};
    require(duckdb_create_config(&config) == DuckDBSuccess, "create config");
    require(duckdb_set_config(config, "autoinstall_known_extensions", "false")
              == DuckDBSuccess,
            "disable extension installation");
    require(duckdb_set_config(config, "allow_community_extensions", "false")
              == DuckDBSuccess,
            "disable community extensions");
    auto* error = static_cast<char*>(nullptr);
    auto status = duckdb_open_ext(nullptr, &db, config, &error);
    duckdb_destroy_config(&config);
    require(status == DuckDBSuccess, error ? error : "open database");
    duckdb_free(error);
    require(duckdb_connect(db, &con) == DuckDBSuccess, "connect");
    require(scalar(con, "SELECT count(*) FROM duckdb_extensions() "
                        "WHERE extension_name IN ('quack', 'httpfs') "
                        "AND loaded AND install_mode = 'STATICALLY_LINKED'")
              == 2,
            "Quack and httpfs must both be compiled in and loaded");
  }

  ~Database() {
    duckdb_disconnect(&con);
    duckdb_close(&db);
  }
};

auto append(duckdb_connection con, int64_t offset, bool query_appender = true)
  -> void {
  auto column_types = std::vector<duckdb_logical_type>{
    duckdb_create_logical_type(DUCKDB_TYPE_BIGINT),
    duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR)};
  auto appender = duckdb_appender{};
  auto status = query_appender
                  ? duckdb_appender_create_query(
                      con,
                      "INSERT INTO remote.main.events (id, payload) "
                      "SELECT * FROM appended_data",
                      column_types.size(), column_types.data(), nullptr,
                      nullptr, &appender)
                  : duckdb_appender_create_ext(con, "remote", "main", "events",
                                               &appender);
  require(status == DuckDBSuccess, duckdb_appender_error(appender));
  if (not query_appender) {
    require(duckdb_appender_add_column(appender, "id") == DuckDBSuccess,
            duckdb_appender_error(appender));
    require(duckdb_appender_add_column(appender, "payload") == DuckDBSuccess,
            duckdb_appender_error(appender));
  }
  auto chunk
    = duckdb_create_data_chunk(column_types.data(), column_types.size());
  for (auto& type : column_types) {
    duckdb_destroy_logical_type(&type);
  }
  // Cross vector boundaries and include nulls, Unicode, and embedded NULs.
  auto const vector_size = duckdb_vector_size();
  for (auto batch = idx_t{0}; batch < 3; ++batch) {
    duckdb_data_chunk_reset(chunk);
    auto const size = batch == 2 ? idx_t{17} : vector_size;
    auto ids = duckdb_data_chunk_get_vector(chunk, 0);
    auto payload = duckdb_data_chunk_get_vector(chunk, 1);
    auto* data = static_cast<int64_t*>(duckdb_vector_get_data(ids));
    duckdb_vector_ensure_validity_writable(payload);
    auto* validity = duckdb_vector_get_validity(payload);
    for (auto i = idx_t{0}; i < size; ++i) {
      data[i] = offset + static_cast<int64_t>(batch * vector_size + i);
      if (i % 2 == 0) {
        duckdb_validity_set_row_invalid(validity, i);
      } else {
        constexpr auto text = "quack 🦆\0payload";
        duckdb_vector_assign_string_element_len(
          payload, i, text, sizeof("quack 🦆\0payload") - 1);
      }
    }
    duckdb_data_chunk_set_size(chunk, size);
    require(duckdb_append_data_chunk(appender, chunk) == DuckDBSuccess,
            duckdb_appender_error(appender));
  }
  duckdb_destroy_data_chunk(&chunk);
  require(duckdb_appender_close(appender) == DuckDBSuccess,
          duckdb_appender_error(appender));
  require(duckdb_appender_destroy(&appender) == DuckDBSuccess,
          "destroy appender");
}

} // namespace

auto main(int argc, char** argv) -> int {
  require(argc == 2 or argc == 4,
          "usage: probe <unused-loopback-port> [tls-port ca-file]");
  auto uri = std::string{"quack:127.0.0.1:"} + argv[1];
  auto client_uri = argc == 4 ? std::string{"quack:127.0.0.1:"} + argv[2] : uri;
  auto server = Database{};
  auto client = Database{};
  query(server.con, "CREATE TABLE events (id BIGINT, payload VARCHAR, "
                    "omitted INTEGER DEFAULT 42)");
  query(server.con, "CALL quack_serve('" + uri + "', token = 'spike-only')");
  query(client.con, "CREATE SECRET (TYPE quack, TOKEN 'spike-only', SCOPE '"
                      + client_uri + "')");
  auto attach = "ATTACH '" + client_uri + "' AS remote (TYPE quack"
                + (argc == 4 ? ", DISABLE_SSL false)" : ")");
  if (argc == 4) {
    query(client.con, "SET enable_server_cert_verification = true");
    auto untrusted = Result{};
    require(duckdb_query(client.con, attach.c_str(), &untrusted.value)
              == DuckDBError,
            "untrusted TLS certificate must be rejected");
    // The harness supplies a generated path, not user SQL.
    query(client.con, std::string{"SET ca_cert_file = '"} + argv[3] + "'");
  }
  query(client.con, attach);
  query(client.con, "USE remote");
  query(client.con, "BEGIN");
  query(client.con, "INSERT INTO events (id) VALUES (-1)");
  query(client.con, "ROLLBACK");
  auto const sql_rollback = scalar(server.con, "SELECT count(*) FROM events");
  std::printf("SQL INSERT rollback: expected 0 rows, got %lld\n",
              static_cast<long long>(sql_rollback));
  query(server.con, "DELETE FROM events");
  query(client.con, "BEGIN");
  append(client.con, 0);
  query(client.con, "COMMIT");
  auto const rows = static_cast<int64_t>(2 * duckdb_vector_size() + 17);
  require(scalar(server.con, "SELECT count(*) FROM events") == rows,
          "committed rows visible on server");
  require(scalar(server.con, "SELECT sum(id) FROM events")
            == rows * (rows - 1) / 2,
          "IDs survive data chunk transfer");
  require(scalar(server.con, "SELECT count(*) FROM events WHERE omitted = 42")
            == rows,
          "omitted column receives its constant default");
  require(scalar(server.con,
                 "SELECT count(*) FROM events WHERE payload IS NULL")
            == static_cast<int64_t>(duckdb_vector_size() + 9),
          "null validity survives data chunk transfer");
  require(scalar(server.con,
                 "SELECT count(*) FROM events "
                 "WHERE payload = 'quack 🦆' || chr(0) || 'payload'")
            == static_cast<int64_t>(duckdb_vector_size() + 8),
          "Unicode and embedded NUL survive data chunk transfer");
  query(client.con, "BEGIN TRANSACTION READ ONLY");
  require(scalar(client.con, "SELECT count(*) FROM events") == rows,
          "read-only transaction can read from the remote default catalog");
  query(client.con, "ROLLBACK");
  query(client.con, "BEGIN");
  append(client.con, rows);
  query(client.con, "ROLLBACK");
  auto const query_rollback = scalar(server.con, "SELECT count(*) FROM events");
  std::printf("query-appender rollback: expected %lld rows, got %lld\n",
              static_cast<long long>(rows),
              static_cast<long long>(query_rollback));
  query(server.con, "DELETE FROM events WHERE id >= " + std::to_string(rows));
  query(client.con, "BEGIN");
  append(client.con, rows, false);
  query(client.con, "COMMIT");
  require(scalar(server.con, "SELECT count(*) FROM events") == 2 * rows,
          "direct appender writes remotely, including a selected column list");
  require(scalar(server.con, "SELECT count(*) FROM events WHERE omitted = 42")
            == 2 * rows,
          "direct appender preserves omitted columns' defaults");
  std::puts("PASS: direct appender with column selection and defaults");
  query(client.con, "BEGIN");
  append(client.con, 2 * rows, false);
  query(client.con, "ROLLBACK");
  auto const direct_rollback
    = scalar(server.con, "SELECT count(*) FROM events");
  std::printf("direct-appender rollback: expected %lld rows, got %lld\n",
              static_cast<long long>(2 * rows),
              static_cast<long long>(direct_rollback));
  query(server.con,
        "DELETE FROM events WHERE id >= " + std::to_string(2 * rows));
  query(client.con, "BEGIN");
  query(client.con, "SELECT count(*) FROM events");
  append(client.con, 2 * rows);
  query(client.con, "ROLLBACK");
  auto const primed_rollback
    = scalar(server.con, "SELECT count(*) FROM events");
  std::printf(
    "query-appender rollback after remote SELECT: expected %lld rows, "
    "got %lld\n",
    static_cast<long long>(2 * rows), static_cast<long long>(primed_rollback));
  query(client.con, "USE memory");
  query(client.con, "DETACH remote");
  query(server.con, "CALL quack_stop('" + uri + "')");
  std::puts("PASS: bundled extensions, query appender, defaults, round trip, "
            "commit, read-only transaction");
  if (sql_rollback != 0 or query_rollback != rows or direct_rollback != 2 * rows
      or primed_rollback != 2 * rows) {
    std::fflush(stdout);
    std::fputs("NOTE: remote rollback is not reliable in this Quack version\n",
               stderr);
  }
}
