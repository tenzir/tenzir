//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/argument_parser.hpp>
#include <tenzir/location.hpp>
#include <tenzir/plugin.hpp>
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

class plugin final : public virtual saver_plugin<abs_saver>,
                     public virtual loader_plugin<abs_loader> {
public:
  auto parse_saver(parser_interface& p) const
    -> std::unique_ptr<plugin_saver> override {
    auto parser = argument_parser{
      name(), fmt::format("https://docs.tenzir.com/connectors/{}", name())};
    auto uri = located<std::string>{};
    parser.add(uri, "<uri>");
    parser.parse(p);
    auto out = std::string{};
    auto opts = arrow::fs::AzureOptions::FromUri(uri.inner, &out);
    if (auto s = opts.status(); s != arrow::Status::OK()) {
      throw diagnostic::error("Failed to parse URI {}", s.ToString())
        .primary(uri);
    }
    return std::make_unique<abs_saver>(std::move(uri));
  }

  auto parse_loader(parser_interface& p) const
    -> std::unique_ptr<plugin_loader> override {
    auto parser = argument_parser{
      name(), fmt::format("https://docs.tenzir.com/connectors/{}", name())};
    auto uri = located<std::string>{};
    parser.add(uri, "<uri>");
    parser.parse(p);
    auto out = std::string{};
    auto opts = arrow::fs::AzureOptions::FromUri(uri.inner, &out);
    if (auto s = opts.status(); s != arrow::Status::OK()) {
      throw diagnostic::error("Failed to parse URI {}", s.ToString())
        .primary(uri);
    }
    return std::make_unique<abs_loader>(std::move(uri));
  }

  auto name() const -> std::string override {
    return "azure-blob-storage";
  }

  auto supported_uri_schemes() const -> std::vector<std::string> override {
    return {"abfs", "abfss"};
  }
};

class load_abs_operator final : public crtp_operator<load_abs_operator> {
public:
  load_abs_operator() = default;

  explicit load_abs_operator(located<std::string> uri) : uri_{std::move(uri)} {
  }

  auto operator()(operator_control_plane& ctrl) const -> generator<chunk_ptr> {
    auto loader = abs_loader{uri_};
    auto instance = loader.instantiate(ctrl);
    if (not instance) {
      co_return;
    }
    for (auto&& chunk : *instance) {
      co_yield std::move(chunk);
    }
  }

  auto name() const -> std::string override {
    return "tql2.load_azure_blob_storage";
  }

  auto location() const -> operator_location override {
    return operator_location::local;
  }

  auto optimize(expression const& filter, event_order order) const
    -> optimize_result override {
    TENZIR_UNUSED(filter, order);
    return do_not_optimize(*this);
  }

  friend auto inspect(auto& f, load_abs_operator& x) -> bool {
    return f.apply(x.uri_);
  }

private:
  located<std::string> uri_;
};

class save_abs_operator final : public crtp_operator<save_abs_operator> {
public:
  save_abs_operator() = default;

  explicit save_abs_operator(located<std::string> uri) : uri_{std::move(uri)} {
  }

  auto
  operator()(generator<chunk_ptr> input, operator_control_plane& ctrl) const
    -> generator<std::monostate> {
    auto saver = abs_saver{uri_};
    auto instance = saver.instantiate(ctrl, {});
    if (not instance) {
      co_return;
    }
    for (auto&& chunk : input) {
      (*instance)(std::move(chunk));
      co_yield {};
    }
  }

  auto name() const -> std::string override {
    return "tql2.save_azure_blob_storage";
  }

  auto location() const -> operator_location override {
    return operator_location::local;
  }

  auto optimize(expression const& filter, event_order order) const
    -> optimize_result override {
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
  auto make(invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    auto uri = located<std::string>{};
    TRY(argument_parser2::operator_("save_azure_blob_storage")
          .add("uri", uri)
          .parse(inv, ctx));
    return std::make_unique<save_abs_operator>(std::move(uri));
  }
};

class load_abs_plugin final : public operator_plugin2<load_abs_operator> {
public:
  auto make(invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    auto uri = located<std::string>{};
    TRY(argument_parser2::operator_("load_azure_blob_storage")
          .add("uri", uri)
          .parse(inv, ctx));
    return std::make_unique<load_abs_operator>(std::move(uri));
  }
};

} // namespace tenzir::plugins::abs

TENZIR_REGISTER_PLUGIN(tenzir::plugins::abs::plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::abs::save_abs_plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::abs::load_abs_plugin)
