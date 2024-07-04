//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/argument_parser.hpp>
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
constexpr size_t max_chunk_size = 1 << 20;
} // namespace

class s3_loader final : public plugin_loader {
public:
  s3_loader() = default;

  s3_loader(s3_args args) : args_{std::move(args)} {
  }
  auto instantiate(operator_control_plane& ctrl) const
    -> std::optional<generator<chunk_ptr>> override {
    return
      [](s3_args args, operator_control_plane& ctrl) -> generator<chunk_ptr> {
        auto uri = arrow::util::Uri{};
        const auto parse_result = uri.Parse(args.uri.inner);
        if (not parse_result.ok()) {
          diagnostic::error("failed to parse URI `{}`: {}", args.uri.inner,
                            parse_result.ToString())
            .primary(args.uri.source)
            .emit(ctrl.diagnostics());
          co_return;
        }
        auto opts = get_options(args);
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
                            args.uri.inner, file_info.status().ToString())
            .primary(args.uri.source)
            .emit(ctrl.diagnostics());
          co_return;
        }
        auto input_stream = fs.ValueUnsafe()->OpenInputStream(*file_info);
        if (not input_stream.ok()) {
          diagnostic::error("failed to open input stream for URI "
                            "`{}`: {}",
                            args.uri.inner, input_stream.status().ToString())
            .primary(args.uri.source)
            .emit(ctrl.diagnostics());
          co_return;
        }
        while (not input_stream.ValueUnsafe()->closed()) {
          auto buffer = input_stream.ValueUnsafe()->Read(max_chunk_size);
          if (not input_stream.ok()) {
            diagnostic::error("failed to read from input stream for URI "
                              "`{}`: {}",
                              args.uri.inner, buffer.status().ToString())
              .primary(args.uri.source)
              .emit(ctrl.diagnostics());
            co_return;
          }
          if (buffer.ValueUnsafe()->size() == 0) {
            break;
          }
          co_yield chunk::make(buffer.MoveValueUnsafe());
        }
      }(args_, ctrl);
  }

  auto name() const -> std::string override {
    return "s3";
  }

  auto default_parser() const -> std::string override {
    return "json";
  }

  friend auto inspect(auto& f, s3_loader& x) -> bool {
    return f.object(x).pretty_name("s3_loader").fields(f.field("args", x.args_));
  }

private:
  s3_args args_;
};

class s3_saver final : public plugin_saver {
public:
  s3_saver() = default;

  s3_saver(s3_args args) : args_{std::move(args)} {
  }

  auto instantiate(operator_control_plane& ctrl, std::optional<printer_info>)
    -> caf::expected<std::function<void(chunk_ptr)>> override {
    auto uri = arrow::util::Uri{};
    const auto parse_result = uri.Parse(args_.uri.inner);
    if (not parse_result.ok()) {
      return caf::make_error(ec::filesystem_error,
                             fmt::format("failed to parse URI `{}`: {}",
                                         args_.uri.inner,
                                         parse_result.ToString()));
    }
    auto opts = get_options(args_);
    if (not opts) {
      return std::move(opts.error());
    }
    auto fs = arrow::fs::S3FileSystem::Make(std::move(*opts));
    if (not fs.ok()) {
      return caf::make_error(ec::filesystem_error,
                             fmt::format("failed to create Arrow S3 "
                                         "filesystem: {}",
                                         fs.status().ToString()));
    }
    auto file_info = fs.ValueUnsafe()->GetFileInfo(
      fmt::format("{}{}", uri.host(), uri.path()));
    if (not file_info.ok()) {
      return caf::make_error(ec::filesystem_error,
                             fmt::format("failed to get file info from path "
                                         "`{}`: {}",
                                         args_.uri.inner,
                                         file_info.status().ToString()));
    }
    auto output_stream = fs.ValueUnsafe()->OpenOutputStream(file_info->path());
    if (not output_stream.ok()) {
      return caf::make_error(ec::filesystem_error,
                             fmt::format("failed to open output stream for URI "
                                         "`{}`: {}",
                                         args_.uri.inner,
                                         output_stream.status().ToString()));
    }
    auto stream_guard
      = caf::detail::make_scope_guard([this, &ctrl, output_stream]() {
          auto status = output_stream.ValueUnsafe()->Close();
          if (not output_stream.ok()) {
            diagnostic::error("{}", status.ToString())
              .note("failed to close stream for URI `{}`", args_.uri.inner)
              .emit(ctrl.diagnostics());
          }
        });
    return [&ctrl, output_stream, uri = args_.uri.inner,
            stream_guard = std::make_shared<decltype(stream_guard)>(
              std::move(stream_guard))](chunk_ptr chunk) mutable {
      if (!chunk || chunk->size() == 0) {
        return;
      }
      auto status
        = output_stream.ValueUnsafe()->Write(chunk->data(), chunk->size());
      if (not output_stream.ok()) {
        diagnostic::error("{}", status.ToString())
          .note("failed to erite to stream for URI `{}`", uri)
          .emit(ctrl.diagnostics());
        return;
      }
    };
  }

  auto name() const -> std::string override {
    return "s3";
  }

  auto default_printer() const -> std::string override {
    return "json";
  }

  auto is_joining() const -> bool override {
    return true;
  }

  friend auto inspect(auto& f, s3_saver& x) -> bool {
    return f.object(x).pretty_name("s3_saver").fields(f.field("args", x.args_));
  }

private:
  s3_args args_;
};

class plugin final : public virtual loader_plugin<s3_loader>,
                     public virtual saver_plugin<s3_saver> {
public:
  ~plugin() noexcept override {
    const auto finalized = arrow::fs::FinalizeS3();
    TENZIR_ASSERT(finalized.ok(), finalized.ToString().c_str());
  }

  auto initialize(const record& plugin_config, const record& global_config)
    -> caf::error override {
    (void)global_config;
    auto initialized = arrow::fs::EnsureS3Initialized();
    if (not initialized.ok()) {
      return caf::make_error(ec::filesystem_error,
                             fmt::format("failed to initialize Arrow S3 "
                                         "functionality: {}",
                                         initialized.ToString()));
    }
    if (plugin_config.empty()) {
      return {};
    }
    config_.emplace();
    for (const auto& [key, value] : plugin_config) {
#define X(opt, var)                                                            \
  if (key == (opt)) {                                                          \
    if (value == data{}) {                                                     \
      continue;                                                                \
    }                                                                          \
    if (const auto* str = caf::get_if<std::string>(&value)) {                  \
      config_->var = *str;                                                     \
      continue;                                                                \
    }                                                                          \
    return diagnostic::error("invalid S3 configuration: {} must be a string",  \
                             key)                                              \
      .note("{} is configured as {}", key, value)                              \
      .to_error();                                                             \
  }
      X("access-key", access_key)
      X("secret-key", secret_key)
      X("session-token", session_token)
#undef X
      return diagnostic::error(
               "invalid S3 configuration: unrecognized option {}", key)
        .note("{} is configured as {}", key, value)
        .to_error();
    }
    return {};
  }

  auto parse_loader(parser_interface& p) const
    -> std::unique_ptr<plugin_loader> override {
    auto parser = argument_parser{
      name(), fmt::format("https://docs.tenzir.com/connectors/{}", name())};
    auto args = s3_args{};
    parser.add("--anonymous", args.anonymous);
    parser.add(args.uri, "<uri>");
    parser.parse(p);
    // TODO: URI parser.
    if (not args.uri.inner.starts_with("s3://")) {
      args.uri.inner = fmt::format("s3://{}", args.uri.inner);
    }
    args.config = config_;
    return std::make_unique<s3_loader>(std::move(args));
  }

  auto parse_saver(parser_interface& p) const
    -> std::unique_ptr<plugin_saver> override {
    auto parser = argument_parser{
      name(), fmt::format("https://docs.tenzir.com/connectors/{}", name())};
    auto args = s3_args{};
    parser.add("--anonymous", args.anonymous);
    parser.add(args.uri, "<uri>");
    parser.parse(p);
    // TODO: URI parser.
    if (not args.uri.inner.starts_with("s3://")) {
      args.uri.inner = fmt::format("s3://{}", args.uri.inner);
    }
    args.config = config_;
    return std::make_unique<s3_saver>(std::move(args));
  }

  auto name() const -> std::string override {
    return "s3";
  }

  std::optional<s3_config> config_ = {};
};

} // namespace tenzir::plugins::s3

TENZIR_REGISTER_PLUGIN(tenzir::plugins::s3::plugin)
