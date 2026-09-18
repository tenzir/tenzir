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

struct FromFileArgs : FromArrowFsArgs {
  Option<location> mmap;
};

template <class Output>
class FromFileOperator final : public FromArrowFsOperator<Output> {
public:
  explicit FromFileOperator(FromFileArgs args)
    : FromArrowFsOperator<Output>{static_cast<FromArrowFsArgs&>(args)},
      args_{std::move(args)} {
  }

protected:
  using FromArrowFsOperator<Output>::filesystem;

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
    if (not std::filesystem::path{expanded}.is_absolute()) {
      expanded = "./" + expanded;
    }
    expanded = std::filesystem::weakly_canonical(expanded);
    // `UriFromAbsolutePath` percent-encodes characters that are illegal in a
    // URI (e.g. a space in a parent directory name), which a manual `file://`
    // prefix would not. `Uri::path()` decodes them again in `make_filesystem`.
    auto uri_string = arrow::util::UriFromAbsolutePath(expanded);
    if (not uri_string.ok()) {
      diagnostic::error("failed to construct file URI")
        .primary(args_.url)
        .note(uri_string.status().ToStringWithoutContextLines())
        .emit(ctx);
      co_return failure::promise();
    }
    auto uri = arrow::util::Uri{};
    auto status = uri.Parse(*uri_string);
    if (not status.ok()) {
      diagnostic::error("failed to parse path as URI")
        .primary(args_.url)
        .note(status.ToStringWithoutContextLines())
        .note("full URI: `{}`", *uri_string)
        .emit(ctx);
      co_return failure::promise();
    }
    co_return std::move(uri);
  }

  auto make_filesystem(arrow::util::Uri const& uri, diagnostic_handler&)
    -> Task<failure_or<MakeFilesystemResult>> override {
    auto opts = arrow::fs::LocalFileSystemOptions::Defaults();
    opts.use_mmap = args_.mmap.is_some();
    co_return MakeFilesystemResult{
      std::make_shared<arrow::fs::LocalFileSystem>(opts),
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
    return "from_file";
  }

  auto describe() const -> Description override {
    // `from_file` relays the events produced by its subpipeline unchanged, so
    // its output element type equals the subpipeline's output. We register
    // both instantiations as implementations and use a custom spawner that
    // inspects the subpipeline to pick the matching one. The subpipeline is
    // fed bytes (`chunk_ptr`), so we infer its output for that input.
    auto d = Describer<FromFileArgs, FromFileOperator<table_slice>,
                       FromFileOperator<nova::Events>>{};
    auto pipe_arg = FromArrowFsArgs::describe_to(d);
    d.named("mmap", &FromFileArgs::mmap);
    // Instances split the discovered files among themselves by path. We cannot
    // restrict this to globbing URLs because `url` is a secret that is only
    // resolved at runtime; instances that end up without files simply finish
    // immediately.
    d.parallelizable();
    d.spawner([pipe_arg]<class Input>(DescribeCtx& ctx)
                -> failure_or<Option<SpawnWith<FromFileArgs, Input>>> {
      if constexpr (not std::same_as<Input, void>) {
        return {};
      } else {
        TRY(auto pipe, ctx.get(pipe_arg));
        TRY(auto output, pipe.inner.infer_type(tag_v<chunk_ptr>, ctx));
        return match(
          output,
          [](tag<nova::Events>) -> Option<SpawnWith<FromFileArgs, Input>> {
            return SpawnWith<FromFileArgs, void>{
              [](FromFileArgs args) -> Box<Operator<void, nova::Events>> {
                return FromFileOperator<nova::Events>{std::move(args)};
              }};
          },
          [](auto) -> Option<SpawnWith<FromFileArgs, Input>> {
            return SpawnWith<FromFileArgs, void>{
              [](FromFileArgs args) -> Box<Operator<void, table_slice>> {
                return FromFileOperator<table_slice>{std::move(args)};
              }};
          });
      }
    });
    return d.without_optimize();
  }
};

} // namespace

} // namespace tenzir::plugins::from_file

TENZIR_REGISTER_PLUGIN(tenzir::plugins::from_file::FromFilePlugin)
