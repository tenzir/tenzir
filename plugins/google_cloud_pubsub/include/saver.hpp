#pragma once
//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "google/cloud/pubsub/blocking_publisher.h"
#include "google/cloud/pubsub/publisher.h"

#include <tenzir/location.hpp>
#include <tenzir/plugin.hpp>

#include <arrow/filesystem/azurefs.h>
#include <arrow/filesystem/filesystem.h>
#include <arrow/filesystem/type_fwd.h>
#include <arrow/io/api.h>
#include <arrow/util/uri.h>
#include <fmt/core.h>

#include "args.hpp"

namespace tenzir::plugins::google_cloud_pubsub {

namespace pubsub = ::google::cloud::pubsub;

class saver final : public plugin_saver {
public:
  saver() = default;

  saver(args args) : args_{std::move(args)} {
  }

  auto instantiate(operator_control_plane& ctrl, std::optional<printer_info>)
    -> caf::expected<std::function<void(chunk_ptr)>> override {
    auto topic = pubsub::Topic(args_.project_id_.inner, args_.topic_id_.inner);
    auto connection = pubsub::MakePublisherConnection(std::move(topic));
    auto pub = pubsub::Publisher(std::move(connection));
    return [&ctrl, publisher = std::move(pub)](chunk_ptr chunk) mutable {
      if (not chunk or chunk->size() == 0) {

        return;
      }
      auto message
        = pubsub::MessageBuilder{}
            .SetData(std::string{reinterpret_cast<const char*>(chunk->data()),
                                 chunk->size()})
            .Build();
      auto future = publisher.Publish(std::move(message));
      auto id = future.get();
      if (not id) {
        diagnostic::warning("{}", *id).emit(ctrl.diagnostics());
      }
    };
  }

  auto name() const -> std::string override {
    return "google-cloud-pubsub";
  }

  auto is_joining() const -> bool override {
    return true;
  }

  friend auto inspect(auto& f, saver& x) -> bool {
    return f.apply(x.args_);
  }

private:
  args args_;
};

} // namespace tenzir::plugins::google_cloud_pubsub
