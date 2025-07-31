//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <tenzir/argument_parser.hpp>
#include <tenzir/detail/scope_guard.hpp>
#include <tenzir/location.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/secret_resolution_utilities.hpp>

#include <arrow/filesystem/filesystem.h>
#include <arrow/filesystem/s3fs.h>
#include <arrow/filesystem/type_fwd.h>
#include <arrow/io/api.h>
#include <arrow/util/uri.h>
#include <fmt/core.h>

namespace tenzir::plugins::s3 {

namespace {

struct s3_config {
  std::string access_key;
  std::string secret_key;
  std::string session_token;

  friend auto inspect(auto& f, s3_config& x) -> bool {
    return f.object(x)
      .pretty_name("s3_config")
      .fields(f.field("access-key", x.access_key),
              f.field("secret-key", x.secret_key),
              f.field("session-token", x.session_token));
  }
};

struct s3_args {
  bool anonymous = {};
  located<secret> uri;
  std::optional<s3_config> config;
  std::optional<located<std::string>> role;

  template <class Inspector>
  friend auto inspect(Inspector& f, s3_args& x) -> bool {
    return f.object(x).pretty_name("s3_args").fields(
      f.field("anonymous", x.anonymous), f.field("uri", x.uri),
      f.field("config", x.config), f.field("role", x.role));
  }
};

auto get_options(const s3_args& args, const arrow::util::Uri& uri)
  -> caf::expected<arrow::fs::S3Options> {
  auto opts = arrow::fs::S3Options::FromUri(uri);
  if (not opts.ok()) {
    return diagnostic::error("failed to parse S3 options: {}",
                             opts.status().ToString())
      .to_error();
  }
  if (args.anonymous) {
    opts->ConfigureAnonymousCredentials();
  } else if (args.role) {
    opts->ConfigureAssumeRoleCredentials(args.role->inner);
  } else if (args.config) {
    opts->ConfigureAccessKey(args.config->access_key, args.config->secret_key,
                             args.config->session_token);
  }
  return opts.MoveValueUnsafe();
}

// We use 2^20 for the upper bound of a chunk size, which exactly matches the
// upper limit defined by execution nodes for transporting events.
// TODO: Get the backpressure-adjusted value at runtime from the execution node.
static constexpr size_t max_chunk_size = 1 << 20;

class s3_loader final : public crtp_operator<s3_loader> {
public:
  s3_loader() = default;

  s3_loader(s3_args args) : args_{std::move(args)} {
  }
  auto operator()(operator_control_plane& ctrl) const -> generator<chunk_ptr> {
    auto uri = arrow::util::Uri{};
    co_yield ctrl.resolve_secrets_must_yield(
      {make_uri_request(args_.uri, "s3://", uri, ctrl.diagnostics())});
    auto opts = get_options(args_, uri);
    if (not opts) {
      diagnostic::error(opts.error()).emit(ctrl.diagnostics());
      co_return;
    }
    auto fs = arrow::fs::S3FileSystem::Make(*opts);
    if (not fs.ok()) {
      diagnostic::error("failed to create Arrow S3 filesystem: {}",
                        fs.status().ToStringWithoutContextLines())
        .emit(ctrl.diagnostics());
      co_return;
    }
    auto file_info = fs.ValueUnsafe()->GetFileInfo(
      fmt::format("{}{}", uri.host(), uri.path()));
    if (not file_info.ok()) {
      diagnostic::error("failed to get file info: {}",
                        file_info.status().ToStringWithoutContextLines())
        .primary(args_.uri.source)
        .emit(ctrl.diagnostics());
      co_return;
    }
    auto input_stream = fs.ValueUnsafe()->OpenInputStream(*file_info);
    if (not input_stream.ok()) {
      diagnostic::error("failed to open input stream: {}",
                        input_stream.status().ToStringWithoutContextLines())
        .primary(args_.uri.source)
        .emit(ctrl.diagnostics());
      co_return;
    }
    while (not input_stream.ValueUnsafe()->closed()) {
      auto buffer = input_stream.ValueUnsafe()->Read(max_chunk_size);
      if (not input_stream.ok()) {
        diagnostic::error("failed to read from input stream: {}",
                          buffer.status().ToStringWithoutContextLines())
          .primary(args_.uri.source)
          .emit(ctrl.diagnostics());
        co_return;
      }
      if (buffer.ValueUnsafe()->size() == 0) {
        break;
      }
      co_yield chunk::make(buffer.MoveValueUnsafe());
    }
  }

  auto detached() const -> bool override {
    return true;
  }

  auto optimize(const expression&, event_order) const
    -> optimize_result override {
    return do_not_optimize(*this);
  }

  auto name() const -> std::string override {
    return "load_s3";
  }

  friend auto inspect(auto& f, s3_loader& x) -> bool {
    return f.object(x).pretty_name("s3_loader").fields(f.field("args", x.args_));
  }

private:
  s3_args args_;
};

class s3_saver final : public crtp_operator<s3_saver> {
public:
  s3_saver() = default;

  s3_saver(s3_args args) : args_{std::move(args)} {
  }

  auto
  operator()(generator<chunk_ptr> input, operator_control_plane& ctrl) const
    -> generator<std::monostate> {
    auto uri = arrow::util::Uri{};
    co_yield ctrl.resolve_secrets_must_yield(
      {make_uri_request(args_.uri, "s3://", uri, ctrl.diagnostics())});
    auto opts = get_options(args_, uri);
    if (not opts) {
      diagnostic::error(opts.error()).emit(ctrl.diagnostics());
    }
    auto fs = arrow::fs::S3FileSystem::Make(*opts);
    if (not fs.ok()) {
      diagnostic::error("failed to create Arrow S3 "
                        "filesystem: {}",
                        fs.status().ToStringWithoutContextLines())
        .emit(ctrl.diagnostics());
    }
    auto file_info = fs.ValueUnsafe()->GetFileInfo(
      fmt::format("{}{}", uri.host(), uri.path()));
    if (not file_info.ok()) {
      diagnostic::error("failed to get file info: {}",
                        file_info.status().ToStringWithoutContextLines())
        .emit(ctrl.diagnostics());
    }
    auto output_stream = fs.ValueUnsafe()->OpenOutputStream(file_info->path());
    if (not output_stream.ok()) {
      diagnostic::error("failed to open output stream: {}",
                        output_stream.status().ToStringWithoutContextLines())
        .emit(ctrl.diagnostics());
    }
    auto stream_guard
      = detail::scope_guard([this, &ctrl, output_stream]() noexcept {
          auto status = output_stream.ValueUnsafe()->Close();
          if (not output_stream.ok()) {
            diagnostic::error("failed to close stream: {}",
                              status.ToStringWithoutContextLines())
              .primary(args_.uri.source)
              .emit(ctrl.diagnostics());
          }
        });
    for (const auto& chunk : input) {
      if (not chunk || chunk->size() == 0) {
        co_yield {};
        continue;
      }
      auto status
        = output_stream.ValueUnsafe()->Write(chunk->data(), chunk->size());
      if (not output_stream.ok()) {
        diagnostic::error("failed to write to stream: {}",
                          status.ToStringWithoutContextLines())
          .primary(args_.uri.source)
          .emit(ctrl.diagnostics());
      }
    }
  }

  auto detached() const -> bool override {
    return true;
  }

  auto optimize(const expression&, event_order) const
    -> optimize_result override {
    return do_not_optimize(*this);
  }

  auto name() const -> std::string override {
    return "save_s3";
  }

  friend auto inspect(auto& f, s3_saver& x) -> bool {
    return f.object(x).pretty_name("s3_saver").fields(f.field("args", x.args_));
  }

private:
  s3_args args_;
};
} // namespace
} // namespace tenzir::plugins::s3
