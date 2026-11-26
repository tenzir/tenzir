//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/plugins/iceberg/types.hpp"

#include <tenzir/async/task.hpp>
#include <tenzir/detail/enum.hpp>
#include <tenzir/diagnostics.hpp>
#include <tenzir/location.hpp>
#include <tenzir/nova/secret.hpp>
#include <tenzir/nova/type_system.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/option.hpp>
#include <tenzir/startup_argument.hpp>

#include <concepts>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace tenzir::plugins::iceberg {

TENZIR_ENUM(aws_catalog_service, glue, s3tables);

/// Connection, storage, and authentication arguments shared by Iceberg
/// operators (`from_iceberg`, `to_iceberg`).
struct CatalogArgs {
  located<std::string> table_id;
  /// The REST catalog endpoint. Deliberately named `catalog` instead of the
  /// database-operator contract's `uri`: the operator involves three
  /// endpoints (catalog, warehouse, S3), so the generic name would not say
  /// which one it is, and the Iceberg ecosystem universally configures "a
  /// catalog".
  StartupArgument<nova::Secret> catalog
    = StartupArgument<nova::Secret>::constant("");
  Option<StartupArgument<nova::Secret>> warehouse;
  Option<StartupArgument<nova::Record>> aws_iam;
  Option<located<std::string>> catalog_aws_service;
  Option<StartupArgument<nova::Secret>> s3_endpoint;
  Option<located<bool>> s3_path_style;
  Option<StartupArgument<nova::Secret>> token;
  Option<StartupArgument<nova::Secret>> gcp_service_account_key;
  Option<located<std::string>> gcp_project;
};

/// Splits "ns.sub_ns.table" into namespace levels and the table name.
auto split_table_id(std::string_view table_id)
  -> std::pair<std::vector<std::string>, std::string>;

/// Returns whether `uri` is an `https://` URL pointing at `googleapis.com` or
/// one of its subdomains (e.g. `biglake.googleapis.com`).
auto is_google_catalog_url(std::string_view uri) -> bool;

/// Handles for the arguments registered by `register_catalog_args`, passed to
/// `validate_catalog_args` inside an operator's `d.validate(...)` callback.
template <class Args>
  requires std::derived_from<Args, CatalogArgs>
struct CatalogArgHandles {
  Argument<Args, located<std::string>> table;
  Argument<Args, StartupArgument<nova::Secret>> catalog;
  Argument<Args, StartupArgument<nova::Secret>> warehouse;
  Argument<Args, StartupArgument<nova::Record>> aws_iam;
  Argument<Args, located<std::string>> catalog_aws_service;
  Argument<Args, StartupArgument<nova::Secret>> s3_endpoint;
  Argument<Args, located<bool>> s3_path_style;
  Argument<Args, StartupArgument<nova::Secret>> token;
  Argument<Args, StartupArgument<nova::Secret>> gcp_service_account_key;
  Argument<Args, located<std::string>> gcp_project;
};

namespace catalog_args_detail {

template <class Args, class Member>
  requires std::derived_from<Args, CatalogArgs>
constexpr auto catalog_member(Member CatalogArgs::* ptr) -> Member Args::* {
  return ptr;
}

struct CatalogArgValidation {
  Option<located<std::string>> table;
  Option<located<std::string>> catalog_aws_service;
  Option<location> catalog_aws_service_loc;
  Option<location> s3_endpoint_loc;
  Option<location> s3_path_style_loc;
  Option<location> token_loc;
  Option<location> gcp_service_account_key_loc;
  Option<location> gcp_project_loc;
};

auto validate_catalog_args(CatalogArgValidation const& args,
                           std::string_view operator_name,
                           diagnostic_handler& dh) -> void;

} // namespace catalog_args_detail

/// Registers the shared `CatalogArgs` arguments on `d`.
template <class Args, class... Impls>
  requires std::derived_from<Args, CatalogArgs>
auto register_catalog_args(Describer<Args, Impls...>& d)
  -> CatalogArgHandles<Args> {
  return CatalogArgHandles<Args>{
    .table = d.positional("table", catalog_args_detail::catalog_member<Args>(
                                     &CatalogArgs::table_id)),
    .catalog = d.named("catalog", catalog_args_detail::catalog_member<Args>(
                                    &CatalogArgs::catalog)),
    .warehouse = d.named("warehouse", catalog_args_detail::catalog_member<Args>(
                                        &CatalogArgs::warehouse)),
    .aws_iam = d.named("aws_iam", catalog_args_detail::catalog_member<Args>(
                                    &CatalogArgs::aws_iam)),
    .catalog_aws_service
    = d.named("catalog_aws_service", catalog_args_detail::catalog_member<Args>(
                                       &CatalogArgs::catalog_aws_service)),
    .s3_endpoint
    = d.named("s3_endpoint", catalog_args_detail::catalog_member<Args>(
                               &CatalogArgs::s3_endpoint)),
    .s3_path_style
    = d.named("s3_path_style", catalog_args_detail::catalog_member<Args>(
                                 &CatalogArgs::s3_path_style)),
    .token = d.named(
      "token", catalog_args_detail::catalog_member<Args>(&CatalogArgs::token)),
    .gcp_service_account_key = d.named(
      "gcp_service_account_key", catalog_args_detail::catalog_member<Args>(
                                   &CatalogArgs::gcp_service_account_key)),
    .gcp_project
    = d.named("gcp_project", catalog_args_detail::catalog_member<Args>(
                               &CatalogArgs::gcp_project)),
  };
}

/// Validates the shared `CatalogArgs` arguments registered via
/// `register_catalog_args`. Call from inside `d.validate(...)`.
template <class Args>
  requires std::derived_from<Args, CatalogArgs>
auto validate_catalog_args(CatalogArgHandles<Args> const& handles,
                           std::string_view operator_name, DescribeCtx& ctx)
  -> void {
  catalog_args_detail::validate_catalog_args(
    catalog_args_detail::CatalogArgValidation{
      .table = ctx.get(handles.table),
      .catalog_aws_service = ctx.get(handles.catalog_aws_service),
      .catalog_aws_service_loc = ctx.get_location(handles.catalog_aws_service),
      .s3_endpoint_loc = ctx.get_location(handles.s3_endpoint),
      .s3_path_style_loc = ctx.get_location(handles.s3_path_style),
      .token_loc = ctx.get_location(handles.token),
      .gcp_service_account_key_loc
      = ctx.get_location(handles.gcp_service_account_key),
      .gcp_project_loc = ctx.get_location(handles.gcp_project),
    },
    operator_name, ctx);
}

/// Resolves secrets, AWS/GCP authentication, and FileIO configuration from
/// `args` into a `CatalogConfig` ready for `open_catalog`. Emits diagnostics
/// and returns `None{}` on failure.
auto resolve_catalog_config(CatalogArgs const& args,
                            std::string_view operator_name, OpCtx& ctx)
  -> Task<Option<CatalogConfig>>;

} // namespace tenzir::plugins::iceberg
