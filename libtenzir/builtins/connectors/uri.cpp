//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/argument_parser.hpp>
#include <tenzir/concept/parseable/tenzir/data.hpp>
#include <tenzir/concept/parseable/tenzir/pipeline.hpp>
#include <tenzir/concept/parseable/to.hpp>
#include <tenzir/defaults.hpp>
#include <tenzir/detail/env.hpp>
#include <tenzir/detail/fdinbuf.hpp>
#include <tenzir/detail/fdoutbuf.hpp>
#include <tenzir/detail/file_path_to_parser.hpp>
#include <tenzir/detail/posix.hpp>
#include <tenzir/detail/string.hpp>
#include <tenzir/diagnostics.hpp>
#include <tenzir/fwd.hpp>
#include <tenzir/logger.hpp>
#include <tenzir/parser_interface.hpp>
#include <tenzir/plugin.hpp>

#include <arrow/filesystem/filesystem.h>
#include <arrow/io/api.h>
#include <arrow/util/uri.h>
#include <caf/detail/scope_guard.hpp>

namespace tenzir::plugins::uri {

namespace {

struct uri_args {
  located<std::string> uri;

  template <class Inspector>
  friend auto inspect(Inspector& f, uri_args& x) -> bool {
    return f.object(x).pretty_name("loader_args").fields(f.field("uri", x.uri));
  }
};

class uri_loader final : public plugin_loader {
public:
  // We use 2^20 for the upper bound of a chunk size, which exactly matches the
  // upper limit defined by execution nodes for transporting events.
  // TODO: Get the backpressure-adjusted value at runtime from the execution node.
  static constexpr size_t max_chunk_size = 1 << 20;

  uri_loader() = default;

  explicit uri_loader(uri_args args) : args_{std::move(args)} {
  }

  auto instantiate(operator_control_plane& ctrl) const
    -> std::optional<generator<chunk_ptr>> override {
    return
      [](uri_args args, operator_control_plane& ctrl) -> generator<chunk_ptr> {
        auto uri = arrow::internal::Uri{};
        const auto parse_result = uri.Parse(args.uri.inner);
        if (not parse_result.ok()) {
          diagnostic::error("failed to parse URI `{}`: {}", args.uri.inner,
                            parse_result.ToString())
            .primary(args.uri.source)
            .emit(ctrl.diagnostics());
          co_return;
        }
        auto fs = arrow::fs::FileSystemFromUri(uri.ToString());
        if (not fs.ok()) {
          diagnostic::error("failed to create filesystem from URI "
                            "`{}`: {}",
                            args.uri.inner, fs.status().ToString())
            .primary(args.uri.source)
            .emit(ctrl.diagnostics());
          co_return;
        }
        auto file_info = fs.ValueUnsafe()->GetFileInfo(uri.path());
        if (not fs.ok()) {
          diagnostic::error("failed to get file info from URI "
                            "`{}`: {}",
                            args.uri.inner, file_info.status().ToString())
            .primary(args.uri.source)
            .emit(ctrl.diagnostics());
          co_return;
        }
        auto input_stream
          = fs.ValueUnsafe()->OpenInputStream(file_info.ValueUnsafe());
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

  auto to_string() const -> std::string override {
    return fmt::format("uri {}", args_.uri.inner);
  };

  auto name() const -> std::string override {
    return "uri";
  }

  friend auto inspect(auto& f, uri_loader& x) -> bool {
    return f.apply(x.args_);
  }

private:
  uri_args args_;
};

class uri_saver final : public plugin_saver {
public:
  uri_saver() = default;
  explicit uri_saver(uri_args args) : args_{std::move(args)} {
  }
  auto name() const -> std::string override {
    return "uri";
  }
  auto instantiate(operator_control_plane& ctrl, std::optional<printer_info>)
    -> caf::expected<std::function<void(chunk_ptr)>> override {
    auto uri = arrow::internal::Uri{};

    const auto parse_result = uri.Parse(args_.uri.inner);
    if (not parse_result.ok()) {
      diagnostic::error("failed to parse URI `{}`: {}", args_.uri.inner,
                        parse_result.ToString())
        .primary(args_.uri.source)
        .emit(ctrl.diagnostics());
    }
    auto fs = arrow::fs::FileSystemFromUri(uri.ToString());
    if (not fs.ok()) {
      diagnostic::error("failed to create filesystem from URI "
                        "`{}`: {}",
                        args_.uri.inner, fs.status().ToString())
        .primary(args_.uri.source)
        .emit(ctrl.diagnostics());
    }
    auto file_info = fs.ValueUnsafe()->GetFileInfo(uri.path());
    if (not fs.ok()) {
      diagnostic::error("failed to get file info from URI "
                        "`{}`: {}",
                        args_.uri.inner, file_info.status().ToString())
        .primary(args_.uri.source)
        .emit(ctrl.diagnostics());
    }
    auto output_stream = fs.ValueUnsafe()->OpenOutputStream(file_info->path());
    if (not output_stream.ok()) {
      diagnostic::error("failed to open output stream for URI "
                        "`{}`: {}",
                        args_.uri.inner, output_stream.status().ToString())
        .primary(args_.uri.source)
        .emit(ctrl.diagnostics());
    }
    auto guard = caf::detail::make_scope_guard([this, &ctrl, output_stream]() {
      auto status = output_stream.ValueUnsafe()->Close();
      if (not output_stream.ok()) {
        diagnostic::error("failed to close output stream for URI "
                          "`{}`: {}",
                          args_.uri.inner, status.ToString())
          .primary(args_.uri.source)
          .emit(ctrl.diagnostics());
      }
    });
    return [this, &ctrl, output_stream,
            guard = std::make_shared<decltype(guard)>(std::move(guard))](
             chunk_ptr chunk) -> void {
      if (not chunk or chunk->size() == 0) {
        return;
      }
      auto status
        = output_stream.ValueUnsafe()->Write(chunk->data(), chunk->size());
      if (not output_stream.ok()) {
        diagnostic::error("failed to write to output stream for URI "
                          "`{}`: {}",
                          args_.uri.inner, status.ToString())
          .primary(args_.uri.source)
          .emit(ctrl.diagnostics());
        return;
      }
    };
  }
  auto is_joining() const -> bool override {
    return true;
  }
  friend auto inspect(auto& f, uri_saver& x) -> bool {
    return f.apply(x.args_);
  }

private:
  uri_args args_;
};

class plugin : public virtual loader_plugin<uri_loader>,
               public virtual saver_plugin<uri_saver> {
public:
  auto name() const -> std::string override {
    return "uri";
  }

  auto parse_loader(parser_interface& p) const
    -> std::unique_ptr<plugin_loader> override {
    auto args = uri_args{};
    auto parser = argument_parser{"uri", "https://docs.tenzir.com/next/"
                                         "connectors/uri"};
    parser.add(args.uri, "<uri>");
    parser.parse(p);
    return std::make_unique<uri_loader>(std::move(args));
  }

  auto parse_saver(parser_interface& p) const
    -> std::unique_ptr<plugin_saver> override {
    auto args = uri_args{};
    auto parser = argument_parser{"uri", "https://docs.tenzir.com/next/"
                                         "connectors/uri"};
    parser.add(args.uri, "<uri>");
    parser.parse(p);
    return std::make_unique<uri_saver>(std::move(args));
  }
};

} // namespace

} // namespace tenzir::plugins::uri

TENZIR_REGISTER_PLUGIN(tenzir::plugins::uri::plugin)
