//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "cloudwatch/client.hpp"

#include <tenzir/aws_credentials.hpp>
#include <tenzir/aws_iam.hpp>
#include <tenzir/detail/env.hpp>

#include <aws/core/client/ClientConfiguration.h>

namespace tenzir::plugins::cloudwatch {

auto make_cloudwatch_client(Option<located<record>> aws_iam,
                            Option<located<std::string>> aws_region,
                            location primary, OpCtx& ctx)
  -> Task<std::optional<CloudWatchClient>> {
  auto iam = aws_iam ? std::optional<located<record>>{*std::move(aws_iam)}
                     : std::nullopt;
  auto region_arg
    = aws_region ? std::optional<located<std::string>>{*std::move(aws_region)}
                 : std::nullopt;
  auto auth
    = co_await resolve_aws_iam_auth(std::move(iam), std::move(region_arg), ctx);
  if (not auth) {
    diagnostic::error("failed to initialize CloudWatch Logs client")
      .primary(primary)
      .emit(ctx);
    co_return std::nullopt;
  }
  auto region = std::optional<std::string>{};
  if (aws_region) {
    region = aws_region->inner;
  } else if (auth->credentials and not auth->credentials->region.empty()) {
    region = auth->credentials->region;
  }
  auto provider = make_aws_credentials_provider(auth->credentials, region);
  if (not provider) {
    diagnostic::error("failed to initialize AWS credentials: {}",
                      provider.error())
      .primary(primary)
      .emit(ctx);
    co_return std::nullopt;
  }
  auto config = Aws::Client::ClientConfiguration{};
  if (region) {
    config.region = *region;
  }
  if (auto endpoint = detail::getenv("AWS_ENDPOINT_URL_LOGS")) {
    config.endpointOverride = *endpoint;
  } else if (auto endpoint = detail::getenv("AWS_ENDPOINT_URL")) {
    config.endpointOverride = *endpoint;
  }
  co_return CloudWatchClient{
    .logs = Arc<Aws::CloudWatchLogs::CloudWatchLogsClient>{std::in_place,
                                                           *provider, config},
  };
}

} // namespace tenzir::plugins::cloudwatch
