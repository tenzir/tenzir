//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/argument_parser2.hpp>
#include <tenzir/arrow_fs.hpp>
#include <tenzir/async/blocking_executor.hpp>
#include <tenzir/aws_credentials.hpp>
#include <tenzir/aws_iam.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/pipeline.hpp>
#include <tenzir/plugin/register.hpp>
#include <tenzir/proxy_settings.hpp>
#include <tenzir/scope_linked.hpp>
#include <tenzir/secret_resolution.hpp>
#include <tenzir/secret_resolution_utilities.hpp>
#include <tenzir/tql2/plugin.hpp>

#include <arrow/filesystem/s3fs.h>
#include <arrow/util/uri.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/S3ClientConfiguration.h>
#include <aws/s3/model/DeleteObjectRequest.h>
#include <caf/actor_from_state.hpp>

#include <memory>
#include <string_view>

namespace tenzir::plugins::s3 {
namespace {

struct FromS3Args : FromArrowFsArgs {
  bool anonymous = false;
  Option<located<record>> aws_iam;
  location operator_location = location::unknown;
};

class FromS3Operator final : public FromArrowFsOperator {
public:
  explicit FromS3Operator(FromS3Args args)
    : FromArrowFsOperator{static_cast<FromArrowFsArgs&>(args)},
      args_{std::move(args)} {
  }

protected:
  auto resolve_url(OpCtx& ctx) -> Task<failure_or<arrow::util::Uri>> override {
    auto uri = arrow::util::Uri{};
    auto requests = std::vector<secret_request>{
      make_uri_request(args_.url, "s3://", uri, ctx.dh()),
    };
    auto result = co_await ctx.resolve_secrets(std::move(requests));
    if (result.is_error()) {
      co_return failure::promise();
    }
    auto aws_iam
      = args_.aws_iam ? Option<located<record>>{*args_.aws_iam} : None{};
    resolved_ = co_await resolve_aws_iam_auth(aws_iam, None{}, ctx);
    if (not resolved_) {
      co_return failure::promise();
    }
    co_return std::move(uri);
  }

  auto make_filesystem(arrow::util::Uri const& uri, diagnostic_handler& dh)
    -> Task<failure_or<MakeFilesystemResult>> override {
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
      auto creds = resolved_ ? resolved_->credentials : None{};
      auto region = Option<std::string>{};
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
    auto proxy_key = opts.scheme == "http"
                       ? std::string_view{"tenzir.http-proxy"}
                       : std::string_view{"tenzir.https-proxy"};
    auto const& ps = get_proxy_settings();
    auto proxy = opts.scheme == "http" ? ps.http_proxy : ps.https_proxy;
    // For normal `s3://bucket/...` URLs the URI host is the bucket name, not
    // the actual S3 endpoint. When `endpoint_override` is set, it is the real
    // connect target and can be matched against `tenzir.no-proxy`.
    if (not opts.endpoint_override.empty()) {
      auto endpoint = opts.endpoint_override;
      if (endpoint.find("://") == std::string::npos) {
        endpoint = opts.scheme + "://" + endpoint;
      }
      auto endpoint_uri = arrow::util::Uri{};
      if (auto status = endpoint_uri.Parse(endpoint);
          status.ok() and not endpoint_uri.host().empty()) {
        if (auto target_proxy
            = proxy_for_target(opts.scheme, endpoint_uri.host())) {
          proxy = *target_proxy;
        } else {
          proxy.reset();
        }
      }
    }
    if (proxy) {
      auto proxy_opts = arrow::fs::S3ProxyOptions::FromUri(proxy->url);
      if (not proxy_opts.ok()) {
        diagnostic::warning("`{}` is not usable for S3; "
                            "running this operator without a proxy",
                            proxy_key)
          .primary(args_.operator_location)
          .note("{}", proxy_opts.status().ToStringWithoutContextLines())
          .emit(dh);
      } else {
        opts.proxy_options = proxy_opts.MoveValueUnsafe();
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
    auto config = Aws::S3::S3ClientConfiguration{};
    config.region = opts.region;
    if (not opts.endpoint_override.empty()) {
      config.endpointOverride = opts.endpoint_override;
      config.useVirtualAddressing = opts.force_virtual_addressing;
    }
    config.scheme = opts.scheme == "http" ? Aws::Http::Scheme::HTTP
                                          : Aws::Http::Scheme::HTTPS;
    // Mirror the proxy that S3FileSystem was constructed with, so DeleteObject
    // goes out the same way the rest of the filesystem does.
    if (not opts.proxy_options.host.empty()) {
      config.proxyHost = opts.proxy_options.host;
      config.proxyPort = opts.proxy_options.port > 0
                           ? static_cast<unsigned>(opts.proxy_options.port)
                           : 0u;
      config.proxyScheme = opts.proxy_options.scheme == "https"
                             ? Aws::Http::Scheme::HTTPS
                             : Aws::Http::Scheme::HTTP;
      if (not opts.proxy_options.username.empty()) {
        config.proxyUserName = opts.proxy_options.username;
      }
      if (not opts.proxy_options.password.empty()) {
        config.proxyPassword = opts.proxy_options.password;
      }
    }
    client_.emplace(opts.credentials_provider, nullptr, config);
    co_return MakeFilesystemResult{
      fs_result.MoveValueUnsafe(),
      std::move(path),
    };
  }

  auto remove_file(std::string const& path, diagnostic_handler& dh) const
    -> Task<void> override {
    TENZIR_ASSERT(client_);
    auto [bucket, key] = split_at_first_slash(path);
    auto request = Aws::S3::Model::DeleteObjectRequest{};
    request.SetBucket(bucket);
    request.SetKey(key);
    auto outcome = co_await spawn_blocking(
      [client = *client_, request = std::move(request)] mutable {
        return client.DeleteObject(request);
      });
    if (not outcome.IsSuccess()) {
      diagnostic::warning("failed to delete `{}`", path)
        .primary(args_.url)
        .note("{}", outcome.GetError().GetMessage())
        .emit(dh);
    }
  }

private:
  FromS3Args args_;
  Option<ResolvedAwsIamAuth> resolved_;
  Option<Aws::S3::S3Client> client_;
};

class from_s3 final : public OperatorPlugin {
public:
  auto name() const -> std::string override {
    return "from_s3";
  }

  auto describe() const -> Description override {
    auto d = Describer<FromS3Args, FromS3Operator>{};
    d.operator_location(&FromS3Args::operator_location);
    auto anon = d.named("anonymous", &FromS3Args::anonymous);
    auto aws_iam_arg = d.named("aws_iam", &FromS3Args::aws_iam);
    FromArrowFsArgs::describe_to(d, [=](DescribeCtx& ctx) {
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
    // Instances split the discovered files among themselves by path. We cannot
    // restrict this to globbing URLs because `url` is a secret that is only
    // resolved at runtime; instances that end up without files simply finish
    // immediately.
    d.parallelizable();
    return d.without_optimize();
  }
};

} // namespace
} // namespace tenzir::plugins::s3

TENZIR_REGISTER_PLUGIN(tenzir::plugins::s3::from_s3)
