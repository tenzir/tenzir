//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "clickhouse/arguments.hpp"
#include "clickhouse/transformers.hpp"
#include "tenzir/arc.hpp"
#include "tenzir/async/mutex.hpp"
#include "tenzir/diagnostics.hpp"
#include "tenzir/table_slice.hpp"
#include "tenzir/tql2/eval.hpp"
#include "tenzir/type.hpp"

#include <caf/actor_system_config.hpp>
#include <clickhouse/client.h>

#include <map>
#include <memory>
#include <mutex>
#include <string_view>

namespace tenzir::plugins::clickhouse {

auto inline has_location(const diagnostic& diag) -> bool {
  for (const auto& a : diag.annotations) {
    if (a.source != location::unknown) {
      return true;
    }
  }
  return false;
}

/// Evaluates the `table` expression against `slice` and splits the slice into
/// maximal contiguous runs of rows that resolve to the same, valid table name.
/// `on_run(table, run)` is invoked once per run with the run's target table
/// name and the corresponding subslice of `slice`; returning failure
/// short-circuits the whole walk. Rows whose `table` value is not a valid
/// non-null string are skipped with a warning and break the current run.
///
/// This is the shared splitting core of the legacy generator sink
/// (`easy_client::insert_dynamic`, which inserts each run directly) and the
/// async operator (`ToClickhouse::process`, which buffers each run).
template <class OnRun>
auto split_into_table_runs(const table_slice& slice,
                           const ast::expression& table, location table_loc,
                           diagnostic_handler& dh, OnRun on_run)
  -> failure_or<void> {
  const auto table_values = eval(table, slice, dh);
  auto begin = int64_t{0};
  for (const auto& tables : table_values.parts()) {
    auto strings = tables.as<string_type>();
    if (not strings) {
      diagnostic::warning("expected `string`, got `{}`", tables.type.kind())
        .primary(table)
        .note("event is skipped")
        .emit(dh);
      begin += tables.length();
      continue;
    }
    const auto& array = *strings->array;
    auto run_begin = int64_t{-1};
    auto run_table = std::string_view{};
    // Hands the contiguous run `[begin+run_begin, begin+rel_end)` (all
    // resolving to `run_table`) to `on_run` and resets the run state.
    auto flush = [&](int64_t rel_end) -> failure_or<void> {
      if (run_begin == -1) {
        return {};
      }
      TRY(on_run(run_table,
                 subslice(slice, static_cast<size_t>(begin + run_begin),
                          static_cast<size_t>(begin + rel_end))));
      run_begin = -1;
      run_table = {};
      return {};
    };
    for (auto i = int64_t{0}; i < array.length(); ++i) {
      if (array.IsNull(i)) {
        TRY(flush(i));
        diagnostic::warning("expected `string`, got `null`")
          .primary(table)
          .note("event is skipped")
          .emit(dh);
        continue;
      }
      const auto table_name = array.GetView(i);
      if (not validate_table_name<false>(table_name, table_loc, dh)) {
        TRY(flush(i));
        continue;
      }
      if (run_begin == -1) {
        run_begin = i;
        run_table = table_name;
        continue;
      }
      if (run_table != table_name) {
        TRY(flush(i));
        run_begin = i;
        run_table = table_name;
      }
    }
    TRY(flush(array.length()));
    begin += array.length();
  }
  TENZIR_ASSERT(static_cast<size_t>(begin) == slice.rows());
  return {};
}

class easy_client {
public:
  struct arguments {
    std::string host;
    located<uint64_t> port = {9000, operator_location};
    std::string user;
    std::string password;
    Option<std::string> default_database = None{};
    bool set_client_default_database = true;
    // Set by the caller via `tls_options::resolve()` before invoking.
    Option<TlsConfig> ssl;
    ast::expression table = {};
    located<enum mode> mode = located{mode::create_append, operator_location};
    Option<located<std::string>> primary = None{};
    // Top-level columns to create as ClickHouse `JSON` columns (only used when
    // creating a table). Empty for the legacy operator.
    std::vector<located<std::string>> json = {};
    // Top-level columns to create as `LowCardinality(<inner>)` columns (only
    // used when creating a table). The inner type is inferred from the first
    // event, so every listed column must be present in it.
    std::vector<located<std::string>> low_cardinality = {};
    location operator_location;

    auto make_options() const -> ::clickhouse::ClientOptions {
      auto opts
        = ::clickhouse::ClientOptions()
            .SetEndpoints({::clickhouse::Endpoint{
              std::string{host}, detail::narrow_cast<uint16_t>(port.inner)}})
            .SetUser(std::string{user})
            .SetPassword(std::string{password})
            .SetDefaultDatabase(set_client_default_database and default_database
                                  ? *default_database
                                  : "");
      TENZIR_ASSERT(ssl);
      if (ssl->tls.inner) {
        auto tls_opts = ::clickhouse::ClientOptions::SSLOptions{};
        tls_opts.SetSkipVerification(ssl->skip_peer_verification.inner);
        auto commands = std::vector<
          ::clickhouse::ClientOptions::SSLOptions::CommandAndValue>{};
        if (auto& x = ssl->cacert) {
          commands.emplace_back("ChainCAFile", x->inner);
        }
        if (auto& x = ssl->certfile) {
          commands.emplace_back("Certificate", x->inner);
        }
        if (auto& x = ssl->keyfile) {
          commands.emplace_back("PrivateKey", x->inner);
        }
        tls_opts.SetConfiguration(commands);
        opts.SetSSLOptions(std::move(tls_opts));
      }
      return opts;
    }
  };

private:
  struct ctor_token {};

public:
  explicit easy_client(arguments args, diagnostic_handler& dh, ctor_token)
    : client_{args.make_options()},
      args_{std::move(args)},
      dh_{dh, [loc = args_.operator_location](diagnostic diag) -> diagnostic {
            if (not has_location(diag)) {
              diag.annotations.emplace_back(true, std::string{}, loc);
            }
            return diag;
          }} {
  }

  // Precondition: `args.ssl` has been populated via `tls_options::resolve()`.
  static auto make(arguments args, diagnostic_handler& dh)
    -> std::shared_ptr<easy_client>;

  /// Like `make`, but wraps the client in an async `Mutex` so it can be shared
  /// between tasks that must not touch the connection concurrently (e.g. an
  /// insert worker and a ping loop). `easy_client` is neither movable nor
  /// copyable, so it is constructed in place inside the `Mutex`.
  ///
  /// Precondition: `args.ssl` has been populated via `tls_options::resolve()`.
  static auto make_locked(arguments args, diagnostic_handler& dh)
    -> Arc<Mutex<easy_client>>;

  auto dh() -> diagnostic_handler& {
    return dh_;
  }

  void ping();

  /// Ensures the client knows the transformations for `table_name` given the
  /// input `schema`, creating the table if necessary. Returns a pointer into
  /// cached state (valid until the next fetch for the same name). This performs
  /// blocking remote round-trips (DESCRIBE / CREATE), so callers on an async
  /// executor should wrap it in `spawn_blocking`.
  auto ensure_transformations(const tenzir::record_type& schema,
                              std::string_view table_name)
    -> failure_or<transformer_record*>;

  /// Inserts a single already-resolved, single-schema slice into `table_name`.
  /// Any field that targets a ClickHouse `JSON` column must already be a
  /// serialized JSON string (see `to_json_string_array`). Blocking; wrap in
  /// `spawn_blocking` on an async executor.
  auto insert(const table_slice& slice, std::string_view table_name,
              std::string_view query_id = ::clickhouse::Query::default_query_id)
    -> failure_or<void>;

  /// Evaluates the `table` expression against `slice`, splits it into contiguous
  /// same-table runs, and inserts each run. Used by the legacy generator sink;
  /// the async operator evaluates the table in `process` instead. Blocking.
  auto insert_dynamic(const table_slice& slice,
                      std::string_view query_id
                      = ::clickhouse::Query::default_query_id)
    -> failure_or<void>;

private:
  auto effective_table_name(std::string_view table_name) const -> std::string;
  /// Lock-free core of `ensure_transformations`; the caller must hold
  /// `client_mutex_` and pass an already-resolved table name.
  auto ensure_transformations_impl(const tenzir::record_type& schema,
                                   std::string_view table_name)
    -> failure_or<transformer_record*>;
  /// Lock-free core of `insert`; the caller must hold `client_mutex_`.
  auto insert_impl(const table_slice& slice, std::string_view table_name,
                   std::string_view query_id) -> failure_or<void>;
  /// Checks the DB if an object of the given kind exists.
  auto remote_check_exists(std::string_view object_kind,
                           std::string_view object_name) -> failure_or<bool>;
  /// Fetches the transformations from remote. Assumes the table exists remotely.
  auto remote_fetch_schema_transformations(std::string_view table_name)
    -> failure_or<transformer_record*>;
  auto remote_create_database(std::string_view database_name) -> void;
  auto
  /// Creates a table with the given schema.
  remote_create_table(const tenzir::record_type& schema,
                      std::string_view table_name)
    -> failure_or<transformer_record*>;

  ::clickhouse::Client client_;
  std::mutex client_mutex_;
  arguments args_;
  transforming_diagnostic_handler dh_;
  detail::heterogeneous_string_hashmap<transformer_record> transformations_;
  dropmask_type dropmask_;
};

inline auto apply_connection_uri(easy_client::arguments& args,
                                 const boost::urls::url_view_base& uri)
  -> void {
  args.host = std::string{uri.host()};
  if (uri.has_port()) {
    args.port
      = located<uint64_t>{uint64_t{uri.port_number()}, location::unknown};
  }
  if (uri.has_userinfo()) {
    args.user = std::string{uri.user()};
  }
  if (uri.has_password()) {
    args.password = std::string{uri.password()};
  }
  auto segments = uri.segments();
  if (not segments.empty()) {
    auto database = std::string{segments.front()};
    if (not database.empty()) {
      args.default_database = std::move(database);
      return;
    }
  }
  args.default_database = None{};
}

} // namespace tenzir::plugins::clickhouse
