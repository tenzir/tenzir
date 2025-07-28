//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/secret_resolution_utilities.hpp>
#include <tenzir/tql2/plugin.hpp>

#include <arrow/filesystem/azurefs.h>
#include <arrow/filesystem/filesystem.h>
#include <arrow/filesystem/type_fwd.h>
#include <arrow/io/api.h>
#include <arrow/util/uri.h>
#include <fmt/core.h>

namespace tenzir::plugins::abs {

struct save_abs_args {
  location op;
  located<secret> uri;
  std::optional<located<secret>> account_key;

  friend auto inspect(auto& f, save_abs_args& x) -> bool {
    return f.object(x).fields(f.field("op", x.op), f.field("uri", x.uri),
                              f.field("account_key", x.account_key));
  }
};

class save_abs_operator final : public crtp_operator<save_abs_operator> {
public:
  save_abs_operator() = default;

  explicit save_abs_operator(save_abs_args args) : args_{std::move(args)} {
  }

  auto
  operator()(generator<chunk_ptr> input, operator_control_plane& ctrl) const
    -> generator<std::monostate> {
    auto uri = arrow::util::Uri{};
    auto account_key = std::string{};
    auto reqs = std::vector{
      make_uri_request(args_.uri, "", uri, ctrl.diagnostics()),
    };
    if (args_.account_key) {
      reqs.emplace_back(make_secret_request("account_key",
                                            args_.account_key.value(),
                                            account_key, ctrl.diagnostics()));
    }
    co_yield ctrl.resolve_secrets_must_yield(std::move(reqs));
    auto path = std::string{};
    auto opts = arrow::fs::AzureOptions::FromUri(uri, &path);
    if (not opts.ok()) {
      diagnostic::error("failed to create Arrow Azure Blob Storage "
                        "filesystem: {}",
                        opts.status().ToStringWithoutContextLines())
        .primary(args_.op)
        .emit(ctrl.diagnostics());
      co_return;
    }
    if (args_.account_key) {
      auto status = opts->ConfigureAccountKeyCredential(account_key);
      if (not status.ok()) {
        diagnostic::error("failed to set account key: {}",
                          status.ToStringWithoutContextLines())
          .primary(*args_.account_key)
          .emit(ctrl.diagnostics());
        co_return;
      }
    }
    auto fs = arrow::fs::AzureFileSystem::Make(*opts);
    if (not fs.ok()) {
      diagnostic::error("failed to create Arrow Azure Blob Storage "
                        "filesystem: {}",
                        fs.status().ToStringWithoutContextLines())
        .primary(args_.op)
        .emit(ctrl.diagnostics());
      co_return;
    }
    auto file_info = fs.ValueUnsafe()->GetFileInfo(path);
    if (not file_info.ok()) {
      diagnostic::error("failed to get file info"
                        "{}",
                        file_info.status().ToStringWithoutContextLines())
        .primary(args_.op)
        .emit(ctrl.diagnostics());
      co_return;
    }
    auto output_stream = fs.ValueUnsafe()->OpenOutputStream(file_info->path());
    if (not output_stream.ok()) {
      diagnostic::error("failed to open output stream: "
                        "{}",
                        output_stream.status().ToStringWithoutContextLines())
        .primary(args_.op)
        .emit(ctrl.diagnostics());
      co_return;
    }
    auto stream_guard
      = detail::scope_guard([this, &ctrl, output_stream]() noexcept {
          auto status = output_stream.ValueUnsafe()->Close();
          if (not output_stream.ok()) {
            diagnostic::error("failed to close stream: {}",
                              status.ToStringWithoutContextLines())
              .primary(args_.op)
              .emit(ctrl.diagnostics());
          }
        });
    for (const auto& chunk : input) {
      if (not chunk || chunk->size() == 0) {
        co_yield {};
        continue;
      }
      auto status = output_stream.ValueUnsafe()->Write(
        chunk->data(), detail::narrow<int64_t>(chunk->size()));
      if (not output_stream.ok()) {
        diagnostic::error("failed to write to stream: {}",
                          status.ToStringWithoutContextLines())
          .primary(args_.op)
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

  auto optimize(expression const&, event_order) const
    -> optimize_result override {
    return do_not_optimize(*this);
  }

  friend auto inspect(auto& f, save_abs_operator& x) -> bool {
    return f.apply(x.args_);
  }

private:
  save_abs_args args_;
};

class save_abs_plugin final : public operator_plugin2<save_abs_operator> {
public:
  auto make(invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    auto args = save_abs_args{};
    args.op = inv.self.get_location();
    TRY(argument_parser2::operator_("save_azure_blob_storage")
          .named("uri", args.uri)
          .named("account_key", args.account_key)
          .parse(inv, ctx));
    return std::make_unique<save_abs_operator>(std::move(args));
  }

  auto save_properties() const -> save_properties_t override {
    return {.schemes = {"abfs", "abfss"}};
  }
};

} // namespace tenzir::plugins::abs

TENZIR_REGISTER_PLUGIN(tenzir::plugins::abs::save_abs_plugin)
