//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <tenzir/azure_auth.hpp>
#include <tenzir/operator_plugin.hpp>

#include <tuple>

namespace tenzir::plugins::abs {

/// The blob operators hand the credential to the Azure SDK, which requests the
/// scope itself, so a user-supplied `azure_auth.scope` would be dead.
constexpr auto azure_auth_support = AzureAuthSupport{.scope = false};

/// `resolve_azure_auth` insists on a scope even though the SDK never sees
/// it, so we pass the scope that the SDK will request on its own.
constexpr auto storage_scope = "https://storage.azure.com/.default";

/// Describe-time checks for the `account_key` and `azure_auth` arguments.
template <class AccountKeyArg, class AzureAuthArg>
auto check_azure_auth_args(DescribeCtx& ctx, AccountKeyArg account_key_arg,
                           AzureAuthArg azure_auth_arg) -> void {
  auto account_key_loc = ctx.get_location(account_key_arg);
  auto azure_auth_loc = ctx.get_location(azure_auth_arg);
  if (account_key_loc and azure_auth_loc) {
    diagnostic::error("`azure_auth` cannot be used with `account_key`")
      .primary(*azure_auth_loc)
      .primary(*account_key_loc)
      .emit(ctx);
  }
  if (auto auth = ctx.get(azure_auth_arg); auth) {
    std::ignore = AzureAuthOptions::from_record(*auth, ctx, azure_auth_support);
  }
}

} // namespace tenzir::plugins::abs
