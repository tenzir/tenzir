//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/argument_parser.hpp>
#include <tenzir/detail/scope_guard.hpp>
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

inline auto get_options(const gcs_args& args) -> arrow::fs::GcsOptions {
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

class gcs_loader final : public crtp_operator<gcs_loader> {
public:
  gcs_loader() = default;

  gcs_loader(gcs_args args) : args_{std::move(args)} {
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
#if ARROW_VERSION_MAJOR < 19
    auto fs = arrow::fs::GcsFileSystem::Make(opts);
#else
    auto fs_result = arrow::fs::GcsFileSystem::Make(opts);
    if (not fs_result.ok()) {
      diagnostic::error("{}", fs_result.status().ToString())
        .note("failed to create GCS filesystem")
        .primary(args_.uri.source)
        .emit(ctrl.diagnostics());
      co_return;
    }
    auto fs = fs_result.MoveValueUnsafe();
#endif
    auto file_info
      = fs->GetFileInfo(fmt::format("{}{}", uri.host(), uri.path()));
    if (not file_info.ok()) {
      diagnostic::error("failed to get file info for URI "
                        "`{}`: {}",
                        args_.uri.inner, file_info.status().ToString())
        .primary(args_.uri.source)
        .emit(ctrl.diagnostics());
      co_return;
    }
    auto input_stream = fs->OpenInputStream(*file_info);
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
    return "load_gcs";
  }

  friend auto inspect(auto& f, gcs_loader& x) -> bool {
    return f.object(x)
      .pretty_name("gcs_loader")
      .fields(f.field("args", x.args_));
  }

private:
  gcs_args args_;
};

class gcs_saver final : public crtp_operator<gcs_saver> {
public:
  gcs_saver() = default;

  gcs_saver(gcs_args args) : args_{std::move(args)} {
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
        .primary(args_.uri.source)
        .emit(ctrl.diagnostics());
    }
    auto opts = get_options(args_);
#if ARROW_VERSION_MAJOR < 19
    auto fs = arrow::fs::GcsFileSystem::Make(opts);
#else
    auto fs_result = arrow::fs::GcsFileSystem::Make(opts);
    if (not fs_result.ok()) {
      diagnostic::error("{}", fs_result.status().ToString())
        .note("failed to create GCS filesystem")
        .primary(args_.uri.source)
        .emit(ctrl.diagnostics());
    }
    auto fs = fs_result.MoveValueUnsafe();
#endif
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
    auto stream_guard = detail::scope_guard(
      [this, &ctrl, output_stream]() noexcept {
        auto status = output_stream.ValueUnsafe()->Close();
        if (not output_stream.ok()) {
          diagnostic::error("failed to close output stream for URI `{}`: {}",
                            args_.uri.inner, output_stream.status().ToString())
            .primary(args_.uri.source)
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
          .note("failed to write to output stream for URI `{}`",
                args_.uri.inner)
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
    return "save_gcs";
  }

  friend auto inspect(auto& f, gcs_saver& x) -> bool {
    return f.object(x).pretty_name("gcs_saver").fields(f.field("args", x.args_));
  }

private:
  gcs_args args_;
};
} // namespace tenzir::plugins::gcs
