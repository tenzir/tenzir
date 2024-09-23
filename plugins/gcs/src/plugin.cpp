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
#include <arrow/filesystem/gcsfs.h>
#include <arrow/filesystem/type_fwd.h>
#include <arrow/io/api.h>
#include <arrow/util/uri.h>
#include <fmt/core.h>

#include <string>

namespace tenzir::plugins::gcs {

namespace {

struct gcs_args {
  bool anonymous;
  located<std::string> uri;
  std::string path;

  template <class Inspector>
  friend auto inspect(Inspector& f, gcs_args& x) -> bool {
    return f.object(x)
      .pretty_name("gcs_args")
      .fields(f.field("anonymous", x.anonymous), f.field("uri", x.uri));
  }
};

auto get_options(gcs_args& args) -> arrow::fs::GcsOptions {
  auto opts = arrow::fs::GcsOptions::Anonymous();
  if (not args.anonymous) {
    // The GcsOptions::FromUri() `out_path` parameter is unnecessary, we are
    // generating our own path using the URI.
    auto opts_from_uri
      = arrow::fs::GcsOptions::FromUri(args.uri.inner, nullptr);
    if (not opts_from_uri.ok()) {
      opts = arrow::fs::GcsOptions::Defaults();
    } else {
      opts = *opts_from_uri;
    }
  }
  return opts;
}

// We use 2^20 for the upper bound of a chunk size, which exactly matches the
// upper limit defined by execution nodes for transporting events.
// TODO: Get the backpressure-adjusted value at runtime from the execution node.
constexpr size_t max_chunk_size = 1 << 20;
} // namespace

class gcs_loader final : public plugin_loader {
public:
  gcs_loader() = default;

  gcs_loader(gcs_args args) : args_{std::move(args)} {
  }
  auto instantiate(exec_ctx ctx) const
    -> std::optional<generator<chunk_ptr>> override {
    return
      [](gcs_args args, exec_ctx ctx) -> generator<chunk_ptr> {
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
        // TODO: As of Arrow 13, arrow::GCSFileSystem::Make() only intializes
        // fields of the filesystem & returns a shared_ptr. This is supposed to
        // be changed to a Result, sometime in the future.
        auto fs = arrow::fs::GcsFileSystem::Make(opts);
        auto file_info
          = fs->GetFileInfo(fmt::format("{}{}", uri.host(), uri.path()));
        if (not file_info.ok()) {
          diagnostic::error("failed to get file info for URI "
                            "`{}`: {}",
                            args.uri.inner, file_info.status().ToString())
            .primary(args.uri.source)
            .emit(ctrl.diagnostics());
          co_return;
        }
        auto input_stream = fs->OpenInputStream(*file_info);
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
    return "gcs";
  }

  auto default_parser() const -> std::string override {
    return "json";
  }

  friend auto inspect(auto& f, gcs_loader& x) -> bool {
    return f.object(x)
      .pretty_name("gcs_loader")
      .fields(f.field("args", x.args_));
  }

private:
  gcs_args args_;
};

class gcs_saver final : public plugin_saver {
public:
  gcs_saver() = default;

  gcs_saver(gcs_args args) : args_{std::move(args)} {
  }

  auto instantiate(exec_ctx ctx, std::optional<printer_info>)
    -> caf::expected<std::function<void(chunk_ptr)>> override {
    auto uri = arrow::util::Uri{};
    const auto parse_result = uri.Parse(args_.uri.inner);
    if (not parse_result.ok()) {
      diagnostic::error("failed to parse URI `{}`: {}", args_.uri.inner,
                        parse_result.ToString())
        .primary(args_.uri.source)
        .emit(ctrl.diagnostics());
    }
    auto opts = get_options(args_);
    // TODO: As of Arrow 13, arrow::GCSFileSystem::Make() only intializes
    // fields of the filesystem & returns a shared_ptr. This is supposed to be
    // changed to a Result, sometime in the future.
    auto fs = arrow::fs::GcsFileSystem::Make(opts);
    auto file_info
      = fs->GetFileInfo(fmt::format("{}{}", uri.host(), uri.path()));
    if (not file_info.ok()) {
      diagnostic::error("failed to get file info from URI `{}`: {}",
                        args_.uri.inner, file_info.status().ToString())
        .primary(args_.uri.source)
        .emit(ctrl.diagnostics());
    }
    auto output_stream
      = fs->OpenOutputStream(file_info->path(), opts.default_metadata);
    if (not output_stream.ok()) {
      diagnostic::error("failed to open output stream for URI `{}`: {}",
                        args_.uri.inner, output_stream.status().ToString())
        .primary(args_.uri.source)
        .emit(ctrl.diagnostics());
    }
    auto stream_guard
      = caf::detail::make_scope_guard([this, &ctrl, output_stream]() {
          auto status = output_stream.ValueUnsafe()->Close();
          if (not output_stream.ok()) {
            diagnostic::error("failed to close output stream for URI `{}`: {}",
                              args_.uri.inner,
                              output_stream.status().ToString())
              .primary(args_.uri.source)
              .emit(ctrl.diagnostics());
          }
        });
    return [&ctrl, output_stream, uri = args_.uri.inner,
            stream_guard = std::make_shared<decltype(stream_guard)>(
              std::move(stream_guard))](chunk_ptr chunk) mutable {
      if (!chunk || chunk->size() == 0)
        return;
      auto status
        = output_stream.ValueUnsafe()->Write(chunk->data(), chunk->size());
      if (not output_stream.ok()) {
        diagnostic::error("{}", status.ToString())
          .note("failed to write to output stream for URI `{}`", uri)
          .emit(ctrl.diagnostics());
        return;
      }
    };
  }

  auto name() const -> std::string override {
    return "gcs";
  }

  auto default_printer() const -> std::string override {
    return "json";
  }

  auto is_joining() const -> bool override {
    return true;
  }

  friend auto inspect(auto& f, gcs_saver& x) -> bool {
    return f.object(x).pretty_name("gcs_saver").fields(f.field("args", x.args_));
  }

private:
  gcs_args args_;
};

class plugin final : public virtual loader_plugin<gcs_loader>,
                     public virtual saver_plugin<gcs_saver> {
public:
  auto initialize([[maybe_unused]] const record& plugin_config,
                  [[maybe_unused]] const record& global_config)
    -> caf::error override {
    return caf::none;
  }

  auto parse_loader(parser_interface& p) const
    -> std::unique_ptr<plugin_loader> override {
    auto parser = argument_parser{
      name(), fmt::format("https://docs.tenzir.com/connectors/{}", name())};
    auto args = gcs_args{};
    parser.add("--anonymous", args.anonymous);
    parser.add(args.uri, "<uri>");
    parser.parse(p);
    // TODO: URI parser.
    if (not args.uri.inner.starts_with("gs://"))
      args.uri.inner = fmt::format("gs://{}", args.uri.inner);
    return std::make_unique<gcs_loader>(std::move(args));
  }

  auto parse_saver(parser_interface& p) const
    -> std::unique_ptr<plugin_saver> override {
    auto parser = argument_parser{
      name(), fmt::format("https://docs.tenzir.com/connectors/{}", name())};
    auto args = gcs_args{};
    parser.add("--anonymous", args.anonymous);
    parser.add(args.uri, "<uri>");
    parser.parse(p);
    // TODO: URI parser.
    if (not args.uri.inner.starts_with("gs://"))
      args.uri.inner = fmt::format("gs://{}", args.uri.inner);
    return std::make_unique<gcs_saver>(std::move(args));
  }

  auto name() const -> std::string override {
    return "gcs";
  }

  auto supported_uri_schemes() const -> std::vector<std::string> override {
    return {"gs"};
  }
};

} // namespace tenzir::plugins::gcs

TENZIR_REGISTER_PLUGIN(tenzir::plugins::gcs::plugin)
