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
class save_abs_operator final : public crtp_operator<save_abs_operator> {
public:
  save_abs_operator() = default;

  explicit save_abs_operator(located<std::string> uri) : uri_{std::move(uri)} {
  }

  auto
  operator()(generator<chunk_ptr> input,
             operator_control_plane& ctrl) const -> generator<std::monostate> {
    co_yield {};
    auto path = std::string{};
    auto opts = arrow::fs::AzureOptions::FromUri(uri_.inner, &path);
    if (not opts.ok()) {
      diagnostic::error("failed to create Arrow Azure Blob Storage "
                        "filesystem: {}",
                        opts.status().ToString())
        .emit(ctrl.diagnostics());
    }
    auto fs = arrow::fs::AzureFileSystem::Make(*opts);
    if (not fs.ok()) {
      diagnostic::error("failed to create Arrow Azure Blob Storage "
                        "filesystem: {}",
                        fs.status().ToString())
        .emit(ctrl.diagnostics());
    }
    auto file_info = fs.ValueUnsafe()->GetFileInfo(path);
    if (not file_info.ok()) {
      diagnostic::error("failed to get file info"
                        "{}",
                        file_info.status().ToString())
        .emit(ctrl.diagnostics());
    }
    auto output_stream = fs.ValueUnsafe()->OpenOutputStream(file_info->path());
    if (not output_stream.ok()) {
      diagnostic::error("failed to open output stream: "
                        "{}",
                        output_stream.status().ToString())
        .primary(uri_)
        .emit(ctrl.diagnostics());
    }
    auto stream_guard
      = detail::scope_guard([this, &ctrl, output_stream]() noexcept {
          auto status = output_stream.ValueUnsafe()->Close();
          if (not output_stream.ok()) {
            diagnostic::error("failed to close stream: {}", status.ToString())
              .primary(uri_)
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
          .note("failed to write to stream for URI `{}`", uri_.inner)
          .emit(ctrl.diagnostics());
      }
    }
  }

  auto detached() const -> bool override {
    return true;
  }

  auto name() const -> std::string override {
    return "tql2.save_azure_blob_storage";
  }

  auto location() const -> operator_location override {
    return operator_location::local;
  }

  auto optimize(expression const& filter,
                event_order order) const -> optimize_result override {
    TENZIR_UNUSED(filter, order);
    return do_not_optimize(*this);
  }

  friend auto inspect(auto& f, save_abs_operator& x) -> bool {
    return f.apply(x.uri_);
  }

private:
  located<std::string> uri_;
};

class save_abs_plugin final : public operator_plugin2<save_abs_operator> {
public:
  auto
  make(invocation inv, session ctx) const -> failure_or<operator_ptr> override {
    auto uri = located<std::string>{};
    TRY(argument_parser2::operator_("save_azure_blob_storage")
          .named("uri", uri)
          .parse(inv, ctx));
    return std::make_unique<save_abs_operator>(std::move(uri));
  }

  auto save_properties() const -> save_properties_t override {
    return {.schemes = {"abfs", "abfss"}};
  }
};

} // namespace tenzir::plugins::abs

TENZIR_REGISTER_PLUGIN(tenzir::plugins::abs::save_abs_plugin)
