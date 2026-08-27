//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/plugin/storage_policy.hpp"

namespace tenzir {

// A policy contributes only the parts it cares about; the rest default to
// "nothing to say", which leaves the catalog on its built-in behavior.

auto storage_policy::maintenance_interval() const -> duration {
  return duration::zero();
}

auto storage_policy::maintenance_action(const uuid&,
                                        const partition_synopsis&) const
  -> Option<storage_action> {
  return None{};
}

auto storage_policy::eviction_weight(const uuid&,
                                     const partition_synopsis&) const
  -> Option<double> {
  return None{};
}

auto storage_policy::eviction_action(const uuid&,
                                     const partition_synopsis&) const
  -> Option<storage_action> {
  return None{};
}

void storage_policy::on_merged(const partition_synopsis_pair&) {
}

void storage_policy::on_replaced(const uuid&,
                                 const std::vector<partition_info>&) {
}

void storage_policy::on_committed(const std::any&, const uuid&,
                                  const std::vector<partition_info>&) {
}

void storage_policy::on_erased(const uuid&) {
}

void storage_policy::on_failed(const std::any&, const uuid&,
                               const caf::error&) {
}

} // namespace tenzir
