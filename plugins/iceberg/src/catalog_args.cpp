//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/plugins/iceberg/catalog_args.hpp"

#include "tenzir/plugins/iceberg/catalog.hpp"

#include <tenzir/amazon.hpp>
#include <tenzir/async/blocking_executor.hpp>
#include <tenzir/aws_credentials.hpp>
#include <tenzir/aws_iam.hpp>
#include <tenzir/config.hpp>
#include <tenzir/detail/assert.hpp>
#include <tenzir/detail/string.hpp>

#include <boost/url/parse.hpp>

#include <algorithm>
#include <ranges>
#include <utility>

namespace tenzir::plugins::iceberg {

auto split_table_id(std::string_view table_id)
  -> std::pair<std::vector<std::string>, std::string> {
  auto parts = tenzir::detail::split(table_id, ".");
  if (parts.empty()) {
    return {{}, {}};
  }
  auto ns = std::vector<std::string>{};
  ns.reserve(parts.size() - 1);
  for (auto const& part : parts | std::views::take(parts.size() - 1)) {
    ns.emplace_back(part);
  }
  return {std::move(ns), std::string{parts.back()}};
}

auto is_google_catalog_url(std::string_view uri) -> bool {
  auto parsed = boost::urls::parse_uri(uri);
  if (not parsed) {
    return false;
  }
  if (not tenzir::detail::ascii_icase_equal(std::string_view{parsed->scheme()},
                                            "https")) {
    return false;
  }
  auto host = tenzir::detail::ascii_tolower(parsed->host());
  if (host.ends_with('.')) {
    host.pop_back();
  }
  if (host == "googleapis.com") {
    return true;
  }
  constexpr auto suffix = std::string_view{".googleapis.com"};
  return host.size() > suffix.size() and host.ends_with(suffix)
         and host[host.size() - suffix.size() - 1] != '.';
}

namespace catalog_args_detail {

auto validate_catalog_args(CatalogArgValidation const& args,
                           std::string_view operator_name,
                           diagnostic_handler& dh) -> void {
  if (args.table) {
    auto parts = tenzir::detail::split(args.table->inner, ".");
    auto const valid
      = parts.size() >= 2
        and std::ranges::none_of(parts, &std::string_view::empty);
    if (not valid) {
      diagnostic::error("`table` must be of the form `namespace.table_name`")
        .primary(args.table->source, "got `{}`", args.table->inner)
        .emit(dh);
    }
  }
  if (args.s3_path_style_loc and not args.s3_endpoint_loc) {
    diagnostic::error("`s3_path_style` requires `s3_endpoint`")
      .primary(*args.s3_path_style_loc)
      .emit(dh);
  }
  if (args.catalog_aws_service) {
    if (not from_string<enum aws_catalog_service>(
          args.catalog_aws_service->inner)) {
      diagnostic::error("`catalog_aws_service` must be `glue` or `s3tables`")
        .primary(args.catalog_aws_service->source, "got `{}`",
                 args.catalog_aws_service->inner)
        .emit(dh);
    }
  }
  if (args.catalog_aws_service_loc and args.token_loc) {
    diagnostic::error("`catalog_aws_service` cannot be combined with `token`")
      .primary(*args.catalog_aws_service_loc)
      .emit(dh);
  }
  auto const has_gcp_args = args.gcp_service_account_key_loc.has_value()
                            or args.gcp_project_loc.has_value();
  if (args.catalog_aws_service_loc and has_gcp_args) {
    diagnostic::error("`catalog_aws_service` cannot be combined with Google "
                      "authentication")
      .primary(*args.catalog_aws_service_loc)
      .emit(dh);
  }
  if (args.token_loc and has_gcp_args) {
    diagnostic::error("`token` cannot be combined with Google authentication")
      .primary(*args.token_loc)
      .note("`gcp_service_account_key` and `gcp_project` use Google "
            "authentication, which mints and refreshes tokens automatically")
      .emit(dh);
  }
#ifndef TENZIR_ICEBERG_GCS
  if (has_gcp_args) {
    auto const loc = args.gcp_service_account_key_loc
                       ? *args.gcp_service_account_key_loc
                       : *args.gcp_project_loc;
    diagnostic::error("this build of `{}` lacks Google Cloud support",
                      operator_name)
      .primary(loc)
      .note("rebuild against an Arrow with `ARROW_GCS=ON` and "
            "google-cloud-cpp installed")
      .emit(dh);
  }
#else
  TENZIR_UNUSED(operator_name);
#endif
}

} // namespace catalog_args_detail

auto resolve_catalog_config(CatalogArgs const& args,
                            std::string_view operator_name, OpCtx& ctx)
  -> Task<Option<CatalogConfig>> {
  auto& dh = ctx.dh();
  auto config = CatalogConfig{};
  auto warehouse = std::string{};
  auto s3_endpoint = std::string{};
  auto token = std::string{};
  auto reveal = [&](Option<StartupArgument<nova::Secret>> const& arg,
                    std::string_view name, std::string& out) -> Task<bool> {
    if (not arg) {
      co_return true;
    }
    auto value = co_await arg->evaluate_string(ctx, name);
    if (not value) {
      co_return false;
    }
    out = std::move(*value);
    co_return true;
  };
  auto catalog_uri = co_await args.catalog.evaluate_string(ctx, "catalog");
  if (not catalog_uri
      or not co_await reveal(args.warehouse, "warehouse", warehouse)
      or not co_await reveal(args.s3_endpoint, "s3_endpoint", s3_endpoint)
      or not co_await reveal(args.token, "token", token)
      or not co_await reveal(args.gcp_service_account_key,
                             "gcp_service_account_key",
                             config.gcp_credentials_json)) {
    co_return None{};
  }
  config.uri = std::move(*catalog_uri);
  auto const is_google_url = is_google_catalog_url(config.uri);
  auto const uses_managed_aws_catalog
    = static_cast<bool>(args.catalog_aws_service);
  if (is_google_url and uses_managed_aws_catalog) {
    diagnostic::error("`catalog_aws_service` cannot be used with a Google "
                      "Cloud catalog URL")
      .primary(args.catalog_aws_service->source)
      .secondary(args.catalog.get_location())
      .emit(dh);
    co_return None{};
  }
  // Enable Google OAuth2 catalog authentication when Google-specific arguments
  // are configured, or automatically for Google Cloud REST catalog endpoints
  // unless another catalog authentication mechanism is configured.
  auto const has_gcp_args
    = args.gcp_service_account_key.has_value() or args.gcp_project.has_value();
  config.gcp_auth
    = has_gcp_args
      or (is_google_url and token.empty() and not uses_managed_aws_catalog);
#ifndef TENZIR_ICEBERG_GCS
  if (config.gcp_auth) {
    auto const loc = args.gcp_service_account_key
                       ? args.gcp_service_account_key->get_location()
                       : (args.gcp_project ? args.gcp_project->source
                                           : args.catalog.get_location());
    diagnostic::error("this build of `{}` lacks Google Cloud support",
                      operator_name)
      .primary(loc)
      .note("rebuild against an Arrow with `ARROW_GCS=ON` and "
            "google-cloud-cpp installed")
      .emit(dh);
    co_return None{};
  }
#else
  TENZIR_UNUSED(operator_name);
#endif
  auto auth = co_await resolve_aws_iam_auth(args.aws_iam, None{}, ctx);
  if (not auth) {
    co_return None{};
  }
  config.warehouse = std::move(warehouse);
  config.use_s3_file_io
    = auth->credentials.has_value() or uses_managed_aws_catalog;
  if (args.catalog_aws_service) {
    auto const service
      = from_string<enum aws_catalog_service>(args.catalog_aws_service->inner);
    TENZIR_ASSERT(service);
    config.aws_catalog_signing_name
      = *service == aws_catalog_service::glue ? "glue" : "s3tables";
  }
  if (not uses_managed_aws_catalog and auth->credentials
      and not auth->credentials->region.empty()) {
    config.properties["client.region"] = auth->credentials->region;
  }
  if (not token.empty()) {
    config.properties["rest.auth.type"] = "oauth2";
    config.properties["token"] = std::move(token);
  }
  if (args.gcp_project) {
    config.gcp_user_project = args.gcp_project->inner;
  }
  auto const needs_provider
    = uses_managed_aws_catalog
      or (auth->credentials
          and (not auth->credentials->role.empty()
               or not auth->credentials->profile.empty()
               or auth->credentials->web_identity.has_value()));
  if (needs_provider) {
    // Profile, assume-role, and web-identity providers talk to STS and the
    // shared config machinery, so the AWS SDK must be initialized for every
    // provider, not just for managed AWS catalogs.
    auto initialized = co_await spawn_blocking([] {
      return ensure_aws_sdk_initialized();
    });
    if (not initialized) {
      auto const source = args.aws_iam ? args.aws_iam->get_location()
                                       : args.catalog_aws_service->source;
      diagnostic::error("failed to initialize AWS authentication: {}",
                        initialized.error().message)
        .primary(source)
        .emit(dh);
      co_return None{};
    }
    // Region resolution consults a named profile's config file, which requires
    // the initialized SDK; that is why it happens here and not alongside the
    // other catalog properties above. It runs for every provider-backed
    // configuration, not just managed AWS catalogs: the S3 FileIO would
    // otherwise sign with the SDK's ambient region instead of the selected
    // profile's region.
    auto credentials = Option<resolved_aws_credentials>{None{}};
    if (auth->credentials) {
      credentials = *auth->credentials;
    }
    auto resolved_region
      = co_await spawn_blocking([credentials = std::move(credentials)]() {
          return amazon::resolve_region(None{}, credentials);
        });
    if (uses_managed_aws_catalog) {
      config.aws_signing_region = resolved_region;
    }
    if (not resolved_region.empty()) {
      config.properties["client.region"] = resolved_region;
    }
    // Use the shared live provider for catalog signing and S3 access. The
    // default chain and STS-backed providers refresh expiring credentials,
    // including profiles backed by credential_process.
    auto region = Option<std::string>{};
    if (not resolved_region.empty()) {
      region = resolved_region;
    }
    auto provider = co_await spawn_blocking(
      [credentials = auth->credentials, region = std::move(region)]() {
        return make_aws_credentials_provider(credentials, region);
      });
    if (not provider) {
      auto const source = args.aws_iam ? args.aws_iam->get_location()
                                       : args.catalog_aws_service->source;
      diagnostic::error(provider.error()).primary(source).emit(dh);
      co_return None{};
    }
    if (auth->credentials and not auth->credentials->role.empty()) {
      // The AWS SDK's assume-role provider calls STS lazily and reduces every
      // failure to empty credentials, without a way to retrieve the STS
      // error. Resolve once now, so that a failed role assumption is
      // reported as such here instead of surfacing later as an anonymous
      // authentication failure of the catalog or the object store. The
      // provider caches the session, so a successful call is not repeated.
      auto const empty = co_await spawn_blocking([provider = *provider]() {
        return provider->GetAWSCredentials().IsEmpty();
      });
      if (empty) {
        auto const source = args.aws_iam ? args.aws_iam->get_location()
                                         : args.catalog.get_location();
        // The role can come from a secret, so name the setting instead.
        diagnostic::error("failed to assume the AWS role configured in "
                          "`assume_role` of `aws_iam`")
          .primary(source)
          .note("AWS STS returned no credentials for the role")
          .hint("check that the role exists, that its trust policy lets the "
                "calling identity assume it (including `external_id`), that "
                "the base credentials are valid and may call "
                "`sts:AssumeRole`, and that the region is correct")
          .emit(dh);
        co_return None{};
      }
    }
    config.aws_credentials_provider = std::move(*provider);
  } else if (auth->credentials) {
    auto const& creds = *auth->credentials;
    if (not creds.access_key_id.empty()) {
      config.properties["s3.access-key-id"] = creds.access_key_id;
    }
    if (not creds.secret_access_key.empty()) {
      config.properties["s3.secret-access-key"] = creds.secret_access_key;
    }
    if (not creds.session_token.empty()) {
      config.properties["s3.session-token"] = creds.session_token;
    }
  }
  if (not s3_endpoint.empty()) {
    config.properties["s3.endpoint"] = std::move(s3_endpoint);
    // Custom S3-compatible endpoints practically always require path-style
    // addressing; virtual-hosted style remains opt-out via `s3_path_style`.
    auto const path_style = not args.s3_path_style or args.s3_path_style->inner;
    config.properties["s3.path-style-access"] = path_style ? "true" : "false";
  }
  co_return config;
}

} // namespace tenzir::plugins::iceberg
