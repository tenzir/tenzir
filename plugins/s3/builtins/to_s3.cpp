//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/arrow_fs.hpp>
#include <tenzir/aws_credentials.hpp>
#include <tenzir/aws_iam.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/plugin/register.hpp>
#include <tenzir/secret_resolution.hpp>

#include <arrow/filesystem/s3fs.h>
#include <arrow/util/uri.h>

namespace tenzir::plugins::s3 {
namespace {

struct ToS3Args : ToArrowFsArgs {
  bool anonymous = false;
  Option<located<record>> aws_iam;
};

class ToS3Operator final : public ToArrowFsOperator {
public:
  explicit ToS3Operator(ToS3Args args)
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
    if (not resolved.starts_with("s3://")) {
      resolved = "s3://" + resolved;
    }
    auto aws_iam = args_.aws_iam
                     ? std::optional<located<record>>{*args_.aws_iam}
                     : std::nullopt;
    resolved_ = co_await resolve_aws_iam_auth(aws_iam, std::nullopt, ctx);
    if (not resolved_) {
      co_return failure::promise();
    }
    co_return std::move(resolved);
  }

  auto make_filesystem(std::string const& url, diagnostic_handler& dh)
    -> Task<failure_or<MakeFilesystemResult>> override {
    auto uri = arrow::util::Uri{};
    auto status = uri.Parse(url);
    if (not status.ok()) {
      diagnostic::error("failed to parse S3 URL as URI")
        .primary(args_.url)
        .note(status.ToStringWithoutContextLines())
        .emit(dh);
      co_return failure::promise();
    }
    auto path = std::string{};
    auto opts_result = arrow::fs::S3Options::FromUri(uri, &path);
    if (not opts_result.ok()) {
      diagnostic::error("failed to create S3 options from URI")
        .primary(args_.url)
        .note(opts_result.status().ToStringWithoutContextLines())
        .emit(dh);
      co_return failure::promise();
    }
    auto opts = opts_result.MoveValueUnsafe();
    if (args_.anonymous) {
      opts.ConfigureAnonymousCredentials();
    } else {
      auto creds = resolved_ ? resolved_->credentials : std::nullopt;
      auto region = std::optional<std::string>{};
      if (creds and not creds->region.empty()) {
        region = creds->region;
        opts.region = *region;
      }
      auto provider = make_aws_credentials_provider(creds, region);
      if (not provider) {
        diagnostic::error(provider.error()).primary(args_.url).emit(dh);
        co_return failure::promise();
      }
      opts.credentials_provider = std::move(*provider);
      if (creds) {
        if (not creds->access_key_id.empty() or not creds->profile.empty()) {
          opts.credentials_kind = arrow::fs::S3CredentialsKind::Explicit;
        } else if (not creds->role.empty()) {
          opts.credentials_kind = arrow::fs::S3CredentialsKind::Role;
        }
      }
    }
    auto fs_result = arrow::fs::S3FileSystem::Make(opts);
    if (not fs_result.ok()) {
      diagnostic::error("failed to create S3 filesystem")
        .primary(args_.url)
        .note(fs_result.status().ToStringWithoutContextLines())
        .emit(dh);
      co_return failure::promise();
    }
    co_return MakeFilesystemResult{fs_result.MoveValueUnsafe(),
                                   std::move(path)};
  }

private:
  ToS3Args args_;
  std::optional<ResolvedAwsIamAuth> resolved_;
};

class ToS3Plugin final : public OperatorPlugin {
public:
  auto name() const -> std::string override {
    return "tql2.to_s3";
  }

  auto describe() const -> Description override {
    auto d = Describer<ToS3Args, ToS3Operator>{};
    auto anon = d.named("anonymous", &ToS3Args::anonymous);
    auto aws_iam_arg = d.named("aws_iam", &ToS3Args::aws_iam);
    ToArrowFsArgs::describe_to(d, [=](DescribeCtx& ctx) {
      auto anon_value = ctx.get(anon).value_or(false);
      auto has_iam = ctx.get_location(aws_iam_arg).has_value();
      if (anon_value and has_iam) {
        diagnostic::error("`anonymous` cannot be used with `aws_iam`")
          .primary(*ctx.get_location(anon))
          .emit(ctx);
      }
      if (auto iam = ctx.get(aws_iam_arg); iam) {
        std::ignore = aws_iam_options::from_record(*iam, ctx);
      }
    });
    return d.without_optimize();
  }
};

} // namespace
} // namespace tenzir::plugins::s3

TENZIR_REGISTER_PLUGIN(tenzir::plugins::s3::ToS3Plugin)
