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

#include <arrow/filesystem/filesystem.h>
#include <arrow/filesystem/s3fs.h>
#include <arrow/filesystem/type_fwd.h>
#include <arrow/io/api.h>
#include <arrow/util/uri.h>
#include <fmt/core.h>

namespace tenzir::plugins::s3 {

namespace {
struct s3_config {
  std::string access_key = {};
  std::string secret_key = {};
  std::string session_token = {};

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
  located<std::string> uri = {};
  std::optional<s3_config> config = {};

  template <class Inspector>
  friend auto inspect(Inspector& f, s3_args& x) -> bool {
    return f.object(x).pretty_name("s3_args").fields(
      f.field("anonymous", x.anonymous), f.field("uri", x.uri),
      f.field("config", x.config));
  }
};

auto get_options(const s3_args& args) -> caf::expected<arrow::fs::S3Options> {
  auto opts = arrow::fs::S3Options::FromUri(args.uri.inner);
  if (not opts.ok()) {
    return diagnostic::error("failed to parse S3 options: {}",
                             opts.status().ToString())
      .to_error();
  }
  if (args.anonymous) {
    opts->ConfigureAnonymousCredentials();
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
    co_yield {};
    auto uri = arrow::util::Uri{};
    const auto parse_result = uri.Parse(args_.uri.inner);
    if (not parse_result.ok()) {
      diagnostic::error("failed to parse URI `{}`: {}", args_.uri.inner,
                        parse_result.ToString())
        .primary(args_.uri.source)
        .emit(ctrl.diagnostics());
      co_return;
    }
    auto opts = get_options(args_);
    if (not opts) {
      diagnostic::error(opts.error()).emit(ctrl.diagnostics());
      co_return;
    }
    auto fs = arrow::fs::S3FileSystem::Make(std::move(*opts));
    if (not fs.ok()) {
      diagnostic::error("failed to create Arrow S3 filesystem: {}",
                        fs.status().ToString())
        .emit(ctrl.diagnostics());
      co_return;
    }
    auto file_info = fs.ValueUnsafe()->GetFileInfo(
      fmt::format("{}{}", uri.host(), uri.path()));
    if (not file_info.ok()) {
      diagnostic::error("failed to get file info for URI "
                        "`{}`: {}",
                        args_.uri.inner, file_info.status().ToString())
        .primary(args_.uri.source)
        .emit(ctrl.diagnostics());
      co_return;
    }
    auto input_stream = fs.ValueUnsafe()->OpenInputStream(*file_info);
    if (not input_stream.ok()) {
      diagnostic::error("failed to open input stream for URI "
                        "`{}`: {}",
                        args_.uri.inner, input_stream.status().ToString())
        .primary(args_.uri.source)
        .emit(ctrl.diagnostics());
      co_return;
    }
    while (not input_stream.ValueUnsafe()->closed()) {
      auto buffer = input_stream.ValueUnsafe()->Read(max_chunk_size);
      if (not input_stream.ok()) {
        diagnostic::error("failed to read from input stream for URI "
                          "`{}`: {}",
                          args_.uri.inner, buffer.status().ToString())
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
    co_yield {};
    auto uri = arrow::util::Uri{};
    const auto parse_result = uri.Parse(args_.uri.inner);
    if (not parse_result.ok()) {
      diagnostic::error("failed to parse URI `{}`: {}", args_.uri.inner,
                        parse_result.ToString())
        .emit(ctrl.diagnostics());
    }
    auto opts = get_options(args_);
    if (not opts) {
      diagnostic::error(opts.error()).emit(ctrl.diagnostics());
    }
    auto fs = arrow::fs::S3FileSystem::Make(std::move(*opts));
    if (not fs.ok()) {
      diagnostic::error("failed to create Arrow S3 "
                        "filesystem: {}",
                        fs.status().ToString())
        .emit(ctrl.diagnostics());
    }
    auto file_info = fs.ValueUnsafe()->GetFileInfo(
      fmt::format("{}{}", uri.host(), uri.path()));
    if (not file_info.ok()) {
      diagnostic::error("failed to get file info from path "
                        "`{}`: {}",
                        args_.uri.inner, file_info.status().ToString())
        .emit(ctrl.diagnostics());
    }
    auto output_stream = fs.ValueUnsafe()->OpenOutputStream(file_info->path());
    if (not output_stream.ok()) {
      diagnostic::error("failed to open output stream for URI "
                        "`{}`: {}",
                        args_.uri.inner, output_stream.status().ToString())
        .emit(ctrl.diagnostics());
    }
    auto stream_guard
      = detail::scope_guard([this, &ctrl, output_stream]() noexcept {
          auto status = output_stream.ValueUnsafe()->Close();
          if (not output_stream.ok()) {
            diagnostic::error("{}", status.ToString())
              .note("failed to close stream for URI `{}`", args_.uri.inner)
              .emit(ctrl.diagnostics());
          }
        });
    for (auto chunk : input) {
      if (! chunk || chunk->size() == 0) {
        co_yield {};
        continue;
      }
      auto status
        = output_stream.ValueUnsafe()->Write(chunk->data(), chunk->size());
      if (not output_stream.ok()) {
        diagnostic::error("{}", status.ToString())
          .note("failed to write to stream for URI `{}`", args_.uri.inner)
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
