//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/plugin.hpp"
#include "vast/system/archive.hpp"
#include "vast/system/node.hpp"
#include "vast/system/node_control.hpp"

namespace vast::system {

// This store plugin wraps the global "archive" so we can
// use a unified API in the transition period.
class global_store_plugin final : public virtual store_plugin {
public:
  using store_plugin::builder_and_header;

  // plugin API
  caf::error initialize(data) override {
    return {};
  }

  [[nodiscard]] const char* name() const override {
    return "global_segment_store";
  };

  // store plugin API
  caf::error setup(const node_actor& node) override {
    caf::scoped_actor self{node.home_system()};
    auto maybe_components = get_node_components<archive_actor>(self, node);
    if (!maybe_components)
      return maybe_components.error();
    archive_ = std::get<0>(*maybe_components);
    return caf::none;
  }

  [[nodiscard]] caf::expected<builder_and_header>
  make_store_builder(const vast::uuid&) const override {
    std::vector<char> empty{};
    return builder_and_header{archive_, vast::chunk::make(std::move(empty))};
  }

  [[nodiscard]] caf::expected<system::store_actor>
  make_store(span<const std::byte>) const override {
    return archive_;
  }

private:
  archive_actor archive_;
};

VAST_REGISTER_PLUGIN(vast::system::global_store_plugin)

} // namespace vast::system
