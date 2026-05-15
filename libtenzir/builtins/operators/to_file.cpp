//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/arrow_fs.hpp>
#include <tenzir/file.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/plugin/register.hpp>
#include <tenzir/secret_resolution.hpp>

#include <arrow/filesystem/localfs.h>

namespace tenzir::plugins::to_file {
namespace {

struct ToFileArgs : ToArrowFsArgs {};

class ToFileOperator final : public ToArrowFsOperator {
public:
  explicit ToFileOperator(ToFileArgs args)
    : ToArrowFsOperator{static_cast<ToArrowFsArgs&>(args)},
      args_{std::move(args)} {
  }

protected:
  auto resolve_url(OpCtx& ctx) -> Task<failure_or<std::string>> override {
    auto resolved = std::string{};
    auto requests = std::vector<secret_request>{
      make_secret_request("url", args_.url, resolved, ctx.dh()),
    };
    CO_TRY(co_await ctx.resolve_secrets(std::move(requests)));
    auto expanded = expand_home(std::move(resolved));
    auto path = std::filesystem::path{expanded};
    if (not path.is_absolute()) {
      expanded = "./" + expanded;
    }
    try {
      // `weakly_canonical` treats `{` and `*` as ordinary path-segment
      // characters, so template placeholders survive unchanged.
      expanded = std::filesystem::weakly_canonical(expanded);
    } catch (std::filesystem::filesystem_error const& e) {
      diagnostic::error("failed to canonicalize path: {}", e.what())
        .primary(args_.url)
        .emit(ctx.dh());
      co_return failure::promise();
    }
    co_return std::move(expanded);
  }

  auto make_filesystem(std::string const& path, diagnostic_handler&)
    -> Task<failure_or<MakeFilesystemResult>> override {
    // `LocalFileSystem` accepts raw filesystem paths; no URI parsing is
    // needed. Skipping the `file://` round-trip avoids having to
    // percent-encode characters like space, `?`, `#`, or `%` that may
    // legitimately occur in user paths.
    co_return MakeFilesystemResult{
      std::make_shared<arrow::fs::LocalFileSystem>(),
      path,
    };
  }

private:
  ToFileArgs args_;
};

class ToFilePlugin final : public OperatorPlugin {
public:
  auto name() const -> std::string override {
    return "tql2.to_file";
  }

  auto describe() const -> Description override {
    auto d = Describer<ToFileArgs, ToFileOperator>{};
    ToArrowFsArgs::describe_to(d);
    return d.invariant_order();
  }
};

} // namespace
} // namespace tenzir::plugins::to_file

TENZIR_REGISTER_PLUGIN(tenzir::plugins::to_file::ToFilePlugin)
