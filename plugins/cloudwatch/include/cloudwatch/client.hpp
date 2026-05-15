//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <tenzir/arc.hpp>
#include <tenzir/async.hpp>
#include <tenzir/fwd.hpp>

#include <aws/logs/CloudWatchLogsClient.h>

#include <optional>

namespace tenzir::plugins::cloudwatch {

struct CloudWatchClient {
  Arc<Aws::CloudWatchLogs::CloudWatchLogsClient> logs;
};

auto make_cloudwatch_client(Option<located<record>> aws_iam,
                            Option<located<std::string>> aws_region,
                            location primary, OpCtx& ctx)
  -> Task<std::optional<CloudWatchClient>>;

} // namespace tenzir::plugins::cloudwatch
