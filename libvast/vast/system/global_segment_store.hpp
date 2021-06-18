//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/plugin.hpp"

namespace vast::system {

class global_store_plugin final : public virtual store_plugin {
public:
  using store_plugin::builder_and_header;

  // plugin API
  caf::error initialize(data) override;

  [[nodiscard]] const char* name() const override;

  // component plugin API
  // system::component_plugin_actor make_component(
  //   system::node_actor::stateful_pointer<system::node_state> node) override;

  // store plugin API
  [[nodiscard]] caf::expected<builder_and_header>
  make_store_builder(filesystem_actor fs, const vast::uuid&) const override;

  [[nodiscard]] caf::expected<system::store_actor>
  make_store(filesystem_actor fs, span<const std::byte>) const override;

  // global archive specific functions
  [[nodiscard]] archive_actor archive() const;

private:
  size_t capacity_;
  size_t max_segment_size_;
  mutable archive_actor archive_;
  mutable shutdownable_store_builder_actor adapter_;
};

} // namespace vast::system
