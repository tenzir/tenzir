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

// We use 2^20 for the upper bound of a chunk size, which exactly matches the
// upper limit defined by execution nodes for transporting events.
// TODO: Get the backpressure-adjusted value at runtime from the execution node.
constexpr size_t max_chunk_size = 1 << 20;

class abs_loader final : public plugin_loader {
public:
  abs_loader() = default;

  abs_loader(located<std::string> uri) : uri_{std::move(uri)} {
  }
  auto instantiate(operator_control_plane& ctrl) const
    -> std::optional<generator<chunk_ptr>> override {
    return [](located<std::string> uri,
              operator_control_plane& ctrl) -> generator<chunk_ptr> {
      auto path = std::string{};
      auto opts = arrow::fs::AzureOptions::FromUri(uri.inner, &path);
      if (not opts.ok()) {
        diagnostic::error("failed to create Arrow Azure Blob Storage "
                          "filesystem: {}",
                          opts.status().ToString())
          .emit(ctrl.diagnostics());
        co_return;
      }
      auto fs = arrow::fs::AzureFileSystem::Make(*opts);
      if (not fs.ok()) {
        diagnostic::error("failed to create Arrow Azure Blob Storage "
                          "filesystem: {}",
                          fs.status().ToString())
          .emit(ctrl.diagnostics());
        co_return;
      }
      auto file_info = fs.ValueUnsafe()->GetFileInfo(path);
      if (not file_info.ok()) {
        diagnostic::error("failed to get file info from path {}",
                          file_info.status().ToString())
          .primary(uri)
          .emit(ctrl.diagnostics());
        co_return;
      }
      auto input_stream = fs.ValueUnsafe()->OpenInputStream(*file_info);
      if (not input_stream.ok()) {
        diagnostic::error("failed to open input stream: {}",
                          input_stream.status().ToString())
          .primary(uri)
          .emit(ctrl.diagnostics());
        co_return;
      }
      while (not input_stream.ValueUnsafe()->closed()) {
        auto buffer = input_stream.ValueUnsafe()->Read(max_chunk_size);
        if (not input_stream.ok()) {
          diagnostic::error("failed to read from input stream: {}",
                            buffer.status().ToString())
            .primary(uri)
            .emit(ctrl.diagnostics());
          co_return;
        }
        if (buffer.ValueUnsafe()->size() == 0) {
          break;
        }
        co_yield chunk::make(buffer.MoveValueUnsafe());
      }
    }(uri_, ctrl);
  }

  auto name() const -> std::string override {
    return "azure-blob-storage";
  }

  auto default_parser() const -> std::string override {
    return "json";
  }

  friend auto inspect(auto& f, abs_loader& x) -> bool {
    return f.apply(x.uri_);
  }

private:
  located<std::string> uri_;
};

} // namespace tenzir::plugins::abs
