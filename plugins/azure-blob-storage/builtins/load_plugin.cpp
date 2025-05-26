//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/tql2/plugin.hpp>

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

class load_abs_operator final : public crtp_operator<load_abs_operator> {
public:
  load_abs_operator() = default;

  explicit load_abs_operator(located<std::string> uri) : uri_{std::move(uri)} {
  }

  auto operator()(operator_control_plane& ctrl) const -> generator<chunk_ptr> {
    co_yield {};
    auto path = std::string{};
    auto opts = arrow::fs::AzureOptions::FromUri(uri_.inner, &path);
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
        .primary(uri_)
        .emit(ctrl.diagnostics());
      co_return;
    }
    auto input_stream = fs.ValueUnsafe()->OpenInputStream(*file_info);
    if (not input_stream.ok()) {
      diagnostic::error("failed to open input stream: {}",
                        input_stream.status().ToString())
        .primary(uri_)
        .emit(ctrl.diagnostics());
      co_return;
    }
    while (not input_stream.ValueUnsafe()->closed()) {
      auto buffer = input_stream.ValueUnsafe()->Read(max_chunk_size);
      if (not input_stream.ok()) {
        diagnostic::error("failed to read from input stream: {}",
                          buffer.status().ToString())
          .primary(uri_)
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

  auto name() const -> std::string override {
    return "tql2.load_azure_blob_storage";
  }

  auto location() const -> operator_location override {
    return operator_location::local;
  }

  auto optimize(expression const& filter,
                event_order order) const -> optimize_result override {
    TENZIR_UNUSED(filter, order);
    return do_not_optimize(*this);
  }

  friend auto inspect(auto& f, load_abs_operator& x) -> bool {
    return f.apply(x.uri_);
  }

private:
  located<std::string> uri_;
};

class load_abs_plugin final : public operator_plugin2<load_abs_operator> {
public:
  auto
  make(invocation inv, session ctx) const -> failure_or<operator_ptr> override {
    auto uri = located<std::string>{};
    TRY(argument_parser2::operator_("load_azure_blob_storage")
          .named("uri", uri)
          .parse(inv, ctx));
    return std::make_unique<load_abs_operator>(std::move(uri));
  }

  auto load_properties() const -> load_properties_t override {
    return {.schemes = {"abfs", "abfss"}};
  }
};

} // namespace tenzir::plugins::abs

TENZIR_REGISTER_PLUGIN(tenzir::plugins::abs::load_abs_plugin)
