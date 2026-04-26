//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <tenzir/aws_credentials.hpp>
#include <tenzir/diagnostics.hpp>
#include <tenzir/http_pool.hpp>
#include <tenzir/location.hpp>

#include <aws/core/auth/signer/AWSAuthV4Signer.h>
#include <aws/sqs/model/Message.h>
#include <folly/executors/IOExecutor.h>

namespace tenzir::plugins::sqs {

/// Async SQS client using proxygen for HTTP transport.
///
/// We use proxygen instead of the AWS SDK's built-in HTTP client because
/// the SDK's long-polling calls (e.g., ReceiveMessage with WaitTimeSeconds)
/// cannot be interrupted, which would block pipeline shutdown.
class AsyncSqsQueue {
public:
  /// Creates an async SQS queue client.
  ///
  /// This constructor is synchronous and does not resolve the queue URL.
  /// Call init() to resolve the queue URL asynchronously.
  explicit AsyncSqsQueue(
    located<std::string> name, std::chrono::seconds poll_time,
    std::string region, std::optional<tenzir::resolved_aws_credentials> creds,
    folly::Executor::KeepAlive<folly::IOExecutor> io_executor);

  /// Resolves the queue URL. Must be called before other operations.
  auto init() -> Task<void>;

  /// Receives messages from the queue.
  auto receive_messages(size_t num_messages, std::chrono::seconds poll_time)
    -> Task<Aws::Vector<Aws::SQS::Model::Message>>;

  /// Sends a message to the queue.
  auto send_message(Aws::String data) -> Task<void>;

  /// Deletes a message from the queue.
  auto delete_message(const Aws::SQS::Model::Message& message)
    -> Task<std::optional<diagnostic>>;

private:
  located<std::string> name_;
  std::string region_;
  std::string endpoint_url_;
  Aws::String url_;
  std::shared_ptr<Aws::Auth::AWSCredentialsProvider> credentials_;
  std::unique_ptr<Aws::Client::AWSAuthV4Signer> signer_;
  Box<HttpPool> pool_;
};

} // namespace tenzir::plugins::sqs
