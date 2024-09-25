//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/location.hpp>
#include <tenzir/plugin.hpp>

#include <arrow/filesystem/azurefs.h>
#include <arrow/filesystem/filesystem.h>
#include <arrow/filesystem/type_fwd.h>
#include <arrow/io/api.h>
#include <arrow/util/uri.h>
#include <fmt/core.h>

namespace tenzir::plugins::abs {
class abs_saver final : public plugin_saver {
public:
  abs_saver() = default;

  abs_saver(located<std::string> uri) : uri_{std::move(uri)} {
  }

  auto instantiate(operator_control_plane& ctrl, std::optional<printer_info>)
    -> caf::expected<std::function<void(chunk_ptr)>> override {
    auto path = std::string{};
    auto opts = arrow::fs::AzureOptions::FromUri(uri_.inner, &path);
    if (not opts.ok()) {
      return diagnostic::error("failed to create Arrow Azure Blob Storage "
                               "filesystem: {}",
                               opts.status().ToString())
        .to_error();
    }
    auto fs = arrow::fs::AzureFileSystem::Make(*opts);
    if (not fs.ok()) {
      return diagnostic::error("failed to create Arrow Azure Blob Storage "
                               "filesystem: {}",
                               fs.status().ToString())
        .to_error();
    }
    auto file_info = fs.ValueUnsafe()->GetFileInfo(path);
    if (not file_info.ok()) {
      return diagnostic::error("failed to get file info"
                               "{}",
                               file_info.status().ToString())
        .to_error();
    }
    auto output_stream = fs.ValueUnsafe()->OpenOutputStream(file_info->path());
    if (not output_stream.ok()) {
      return diagnostic::error("failed to open output stream: "
                               "{}",
                               output_stream.status().ToString())
        .primary(uri_)
        .to_error();
    }
    auto stream_guard
      = caf::detail::make_scope_guard([this, &ctrl, output_stream]() {
          auto status = output_stream.ValueUnsafe()->Close();
          if (not output_stream.ok()) {
            diagnostic::error("failed to close stream: {}", status.ToString())
              .primary(uri_)
              .emit(ctrl.diagnostics());
          }
        });
    return [&ctrl, output_stream, uri = uri_,
            stream_guard = std::make_shared<decltype(stream_guard)>(
              std::move(stream_guard))](chunk_ptr chunk) mutable {
      if (!chunk || chunk->size() == 0) {
        return;
      }
      auto status
        = output_stream.ValueUnsafe()->Write(chunk->data(), chunk->size());
      if (not output_stream.ok()) {
        diagnostic::error("{}", status.ToString())
          .note("failed to write to stream for URI `{}`", uri)
          .emit(ctrl.diagnostics());
        return;
      }
    };
  }

  auto name() const -> std::string override {
    return "azure-blob-storage";
  }

  auto default_printer() const -> std::string override {
    return "json";
  }

  auto is_joining() const -> bool override {
    return true;
  }

  friend auto inspect(auto& f, abs_saver& x) -> bool {
    return f.apply(x.uri_);
  }

private:
  located<std::string> uri_;
};

} // namespace tenzir::plugins::abs