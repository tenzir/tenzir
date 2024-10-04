#pragma once
//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

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

// We use 2^20 for the upper bound of a chunk size, which exactly matches the
// upper limit defined by execution nodes for transporting events.
// TODO: Get the backpressure-adjusted value at runtime from the execution node.
constexpr size_t max_chunk_size = 1 << 20;

class loader final : public plugin_loader {
public:
  loader() = default;

  loader(args args) : args_{std::move(args)} {
  }
  auto instantiate(operator_control_plane& ctrl) const
    -> std::optional<generator<chunk_ptr>> override {
    return std::nullopt;
  }

  auto name() const -> std::string override {
    return "google-cloud-pubsub";
  }

  friend auto inspect(auto& f, loader& x) -> bool {
    return f.apply(x.args_);
  }

private:
  args args_;
};

} // namespace tenzir::plugins::google_cloud_pubsub
