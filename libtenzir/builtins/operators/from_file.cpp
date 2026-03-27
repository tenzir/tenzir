//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/arrow_fs.hpp>
#include <tenzir/async/blocking_executor.hpp>
#include <tenzir/file.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/plugin/register.hpp>
#include <tenzir/secret_resolution.hpp>

#include <arrow/filesystem/localfs.h>

namespace tenzir::plugins::from_file {

namespace {

struct FromFileArgs : ArrowFsArgs {};

class FromFileOperator final : public ArrowFsOperator {
public:
  explicit FromFileOperator(FromFileArgs args)
    : ArrowFsOperator{static_cast<ArrowFsArgs&>(args)}, args_{std::move(args)} {
  }

protected:
  auto resolve_url(OpCtx& ctx) -> Task<failure_or<arrow::util::Uri>> override {
    auto resolved = std::string{};
    auto requests = std::vector<secret_request>{
      make_secret_request("url", args_.url, resolved, ctx.dh()),
    };
    auto result = co_await ctx.resolve_secrets(std::move(requests));
    if (result.is_error()) {
      co_return failure::promise();
    }
    auto expanded = expand_home(std::move(resolved));
    expanded = std::filesystem::weakly_canonical(expanded);
    auto uri = arrow::util::Uri{};
    auto status = uri.Parse(fmt::format("file://{}", expanded));
    if (not status.ok()) {
      diagnostic::error("failed to parse path as URI")
        .primary(args_.url)
        .note(status.ToStringWithoutContextLines())
        .emit(ctx);
      co_return failure::promise();
    }
    co_return std::move(uri);
  }

  auto make_filesystem(arrow::util::Uri const& uri, diagnostic_handler&)
    -> Task<failure_or<MakeFilesystemResult>> override {
    co_return MakeFilesystemResult{
      std::make_shared<arrow::fs::LocalFileSystem>(),
      uri.path(),
    };
  }

  auto remove_file(std::string const& path, diagnostic_handler& dh) const
    -> Task<void> override {
    auto status = co_await spawn_blocking([&] {
      return filesystem()->DeleteFile(path);
    });
    if (not status.ok()) {
      diagnostic::warning("failed to delete `{}`", path)
        .primary(args_.url)
        .note(status.ToStringWithoutContextLines())
        .emit(dh);
    }
  }

private:
  FromFileArgs args_;
};

class FromFilePlugin final : public OperatorPlugin {
public:
  auto name() const -> std::string override {
    return "tql2.from_file";
  }

  auto describe() const -> Description override {
    auto d = Describer<FromFileArgs, FromFileOperator>{};
    ArrowFsArgs::describe_to(d);
    return d.without_optimize();
  }
};

} // namespace

} // namespace tenzir::plugins::from_file

TENZIR_REGISTER_PLUGIN(tenzir::plugins::from_file::FromFilePlugin)
