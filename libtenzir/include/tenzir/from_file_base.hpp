//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/argument_parser2.hpp"

#include <tenzir/actors.hpp>
#include <tenzir/glob.hpp>
#include <tenzir/pipeline.hpp>
#include <tenzir/tql2/ast.hpp>
#include <tenzir/tql2/set.hpp>

#include <arrow/filesystem/filesystem.h>
#include <boost/unordered_set.hpp>

namespace tenzir {

struct chunk_source_traits {
  using signatures = caf::type_list<auto(atom::get)->caf::result<chunk_ptr>>;
};

using chunk_source_actor = caf::typed_actor<chunk_source_traits>;

class arrow_chunk_source {
public:
  explicit arrow_chunk_source(std::shared_ptr<arrow::io::InputStream> stream)
    : stream_{std::move(stream)} {
  }

  auto make_behavior() -> chunk_source_actor::behavior_type {
    return {
      [this](atom::get) -> caf::result<chunk_ptr> {
        auto buffer = stream_->Read(1 << 20);
        if (not buffer.ok()) {
          return diagnostic::error(
                   "{}", buffer.status().ToStringWithoutContextLines())
            .to_error();
        }
        return chunk::make(buffer.MoveValueUnsafe());
      },
    };
  }

private:
  std::shared_ptr<arrow::io::InputStream> stream_;
};

struct from_file_args {
  location oploc;
  located<secret> url;
  bool watch{false};
  located<bool> remove{false, location::unknown};
  std::optional<ast::lambda_expr> rename;
  std::optional<ast::field_path> path_field;
  std::optional<located<pipeline>> pipe;

  auto add_to(argument_parser2& p) -> void;
  auto handle(session ctx) const -> failure_or<pipeline>;

  friend auto inspect(auto& f, from_file_args& x) -> bool {
    return f.object(x).fields(f.field("oploc", x.oploc), f.field("url", x.url),
                              f.field("watch", x.watch),
                              f.field("remove", x.remove),
                              f.field("move", x.rename),
                              f.field("path_field", x.path_field),
                              f.field("pipe", x.pipe));
  }
};

struct file_hasher {
  auto operator()(const arrow::fs::FileInfo& file) const -> size_t {
    return hash(file.path(), file.type(), file.size(), file.mtime());
  }
};

using file_set = boost::unordered_set<arrow::fs::FileInfo, file_hasher>;

struct from_file_actor_traits {
  using signatures = caf::type_list<
    // Fetch the result of one of the subpipelines.
    auto(atom::get)->caf::result<table_slice>,
    // Provide a result from one of the subpipelines.
    auto(atom::put, table_slice)->caf::result<void>
    // Derive the metric and diagnostic handler signatures.
    >::append_from<receiver_actor<diagnostic>::signatures,
                   metrics_receiver_actor::signatures>;
};

using from_file_actor = caf::typed_actor<from_file_actor_traits>;

class from_file_state {
public:
  from_file_state(from_file_actor::pointer self, from_file_args args,
                  std::string plaintext_url, event_order order,
                  std::unique_ptr<diagnostic_handler> dh,
                  std::string definition, node_actor node, bool is_hidden,
                  metrics_receiver_actor metrics_receiver,
                  uint64_t operator_index);
  auto make_behavior() -> from_file_actor::behavior_type;

private:
  auto get() -> caf::result<table_slice>;
  auto put(table_slice slice) -> caf::result<void>;
  auto query_files() -> void;
  auto process_file(arrow::fs::FileInfo file) -> void;
  auto got_all_files() -> void;
  auto check_termination() -> void;
  auto check_jobs() -> void;
  auto check_jobs_and_termination() -> void;
  auto add_job(arrow::fs::FileInfo file) -> void;
  auto make_pipeline(std::string_view path) -> failure_or<pipeline>;
  auto start_job(const arrow::fs::FileInfo& file) -> void;
  auto
  start_stream(arrow::Result<std::shared_ptr<arrow::io::InputStream>> stream,
               pipeline pipe, std::string path) -> void;
  auto register_metrics(uint64_t nested_operator_index, uuid nested_metrics_id,
                        type schema) -> caf::result<void>;
  auto handle_metrics(uint64_t nested_operator_index, uuid nested_metrics_id,
                      record metrics) -> caf::result<void>;
  auto pipeline_failed(caf::error error) const -> diagnostic_builder;
  auto is_globbing() const -> bool;

  template <class... Ts>
  auto pipeline_failed(fmt::format_string<Ts...> str, Ts&&... xs) const
    -> diagnostic_builder {
    if (is_globbing()) {
      return diagnostic::warning(std::move(str), std::forward<Ts>(xs)...);
    }
    return diagnostic::error(std::move(str), std::forward<Ts>(xs)...);
  }

  from_file_actor::pointer self_;
  std::unique_ptr<diagnostic_handler> dh_;
  std::shared_ptr<arrow::fs::FileSystem> fs_;

  // The configuration and things derived from it.
  from_file_args args_;
  event_order order_;
  glob glob_;
  std::string root_path_;

  // Watching is implemented by checking against the files seen previously.
  file_set previous_;
  file_set current_;

  // Communication with the operator bridges.
  std::deque<caf::typed_response_promise<table_slice>> gets_;
  std::deque<std::pair<table_slice, caf::typed_response_promise<void>>> puts_;

  // Information needed for spawning subpipelines.
  std::string definition_;
  node_actor node_;
  bool is_hidden_;

  // Job management.
  size_t active_jobs_ = 0;
  std::deque<arrow::fs::FileInfo> jobs_;
  bool added_all_jobs_ = false;

  // Forwarding metrics.
  uint64_t operator_index_ = 0;
  metrics_receiver_actor metrics_receiver_;
};

class from_file_source final : public crtp_operator<from_file_source> {
public:
  from_file_source();

  explicit from_file_source(chunk_source_actor source);

  auto name() const -> std::string override;

  auto operator()(operator_control_plane& ctrl) const -> generator<chunk_ptr> {
    while (true) {
      auto result = chunk_ptr{};
      ctrl.set_waiting(true);
      ctrl.self()
        .mail(atom::get_v)
        .request(source_, caf::infinite)
        .then(
          [&](chunk_ptr chunk) {
            result = std::move(chunk);
            ctrl.set_waiting(false);
          },
          [&](caf::error error) {
            diagnostic::error(std::move(error)).emit(ctrl.diagnostics());
          });
      co_yield {};
      if (not result or result->size() == 0) {
        break;
      }
      co_yield std::move(result);
    }
  }

  auto optimize(const expression&, event_order) const
    -> optimize_result override {
    return do_not_optimize(*this);
  }

  friend auto inspect(auto& f, from_file_source& x) -> bool {
    return f.apply(x.source_);
  }

private:
  chunk_source_actor source_;
};

class from_file_sink final : public crtp_operator<from_file_sink> {
public:
  from_file_sink() = default;

  explicit from_file_sink(
    from_file_actor parent, event_order order,
    std::optional<std::pair<ast::field_path, std::string>> path_field);

  auto name() const -> std::string override;

  auto
  operator()(generator<table_slice> input, operator_control_plane& ctrl) const
    -> generator<std::monostate> {
    for (auto slice : input) {
      if (slice.rows() == 0) {
        co_yield {};
        continue;
      }
      if (path_field_) {
        slice = assign(path_field_->first,
                       data_to_series(path_field_->second, slice.rows()), slice,
                       ctrl.diagnostics());
      }
      // We wait for a response in order to get backpressure.
      ctrl.set_waiting(true);
      ctrl.self()
        .mail(atom::put_v, std::move(slice))
        .request(parent_, caf::infinite)
        .then(
          [&]() {
            ctrl.set_waiting(false);
          },
          [&](caf::error error) {
            diagnostic::error(std::move(error)).emit(ctrl.diagnostics());
          });
      co_yield {};
    }
  }

  auto optimize(const expression&, event_order) const
    -> optimize_result override {
    return optimize_result{std::nullopt, order_, copy()};
  }

  friend auto inspect(auto& f, from_file_sink& x) -> bool {
    return f.object(x).fields(f.field("parent", x.parent_),
                              f.field("order", x.order_),
                              f.field("path_field", x.path_field_));
  }

private:
  from_file_actor parent_;
  event_order order_{};
  std::optional<std::pair<ast::field_path, std::string>> path_field_;
};

} // namespace tenzir
