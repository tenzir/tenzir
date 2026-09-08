//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/plugin/storage_policy.hpp"

#include "tenzir/error.hpp"
#include "tenzir/io/save.hpp"

#include <mutex>

namespace tenzir {

auto invalidate_policy_history(const std::filesystem::path& database_dir)
  -> caf::error {
  // Parallel offline merges share io::save's temporary file name.
  static auto mutex = std::mutex{};
  const auto guard = std::scoped_lock{mutex};
  const auto path = database_dir / invalid_policy_history_path;
  auto error = std::error_code{};
  const auto exists = std::filesystem::exists(path, error);
  if (error) {
    return caf::make_error(ec::filesystem_error,
                           fmt::format("failed to probe {}: {}", path, error));
  }
  if (exists) {
    return {};
  }
  return io::save(path, as_bytes(std::string_view{
                          "Partitions were replaced without a storage policy. "
                          "Reset the stale policy history before removing this "
                          "file.\n"}));
}

// A policy contributes only the parts it cares about; the rest default to
// "nothing to say", which leaves the catalog on its built-in behavior.

auto storage_policy::maintenance_interval() const -> duration {
  return duration::zero();
}

auto storage_policy::maintenance_action(const uuid&, const partition_synopsis&,
                                        time) const -> Option<storage_action> {
  return None{};
}

auto storage_policy::describe() const -> record {
  return {};
}

auto storage_policy::blocks_rebuild(uuid const& id,
                                    partition_synopsis const& synopsis,
                                    time now) const -> bool {
  return maintenance_interval() > duration::zero()
         and maintenance_action(id, synopsis, now).has_value();
}

auto storage_policy::maintenance_deadline(const uuid&,
                                          const partition_synopsis&,
                                          time now) const -> time {
  auto const interval = maintenance_interval();
  return interval > duration::zero() ? now + interval : time::max();
}

auto storage_policy::rule_names() const -> std::vector<std::string> {
  return {};
}

auto storage_policy::check_named_run(std::string_view, Option<duration>,
                                     Option<duration>) const -> caf::error {
  return {};
}

auto storage_policy::named_action(std::string_view, Option<duration>,
                                  Option<duration>, const uuid&,
                                  const partition_synopsis&, time) const
  -> Option<storage_action> {
  return None{};
}

auto storage_policy::eviction_weight(const uuid&, const partition_synopsis&,
                                     time) const -> Option<double> {
  return None{};
}

auto storage_policy::eviction_action(const uuid&,
                                     const partition_synopsis&) const
  -> eviction_action_result {
  return eviction_fallback::erase;
}

void storage_policy::on_merged(const partition_synopsis_pair&) {
}

void storage_policy::on_replaced(const std::vector<uuid>&,
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

auto storage_policy::flush() -> caf::error {
  return {};
}

auto storage_policy::serialize_token(const std::any&) const -> std::string {
  return {};
}

auto storage_policy::deserialize_token(std::string_view) const -> std::any {
  return {};
}

} // namespace tenzir
