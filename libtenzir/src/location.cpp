//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/location.hpp"

#include "tenzir/detail/narrow.hpp"
#include "tenzir/source.hpp"

#include <algorithm>
#include <optional>

namespace tenzir {

struct SourceMap::Impl {
  std::vector<Arc<const Source>> sources;
  std::vector<location> call_sites;
  std::optional<Arc<const Source>> primary_source;
};

SourceMap::SourceMap() : impl_{std::in_place} {
}

SourceMap::SourceMap(const SourceMap&) = default;

SourceMap::SourceMap(SourceMap&&) noexcept = default;

auto SourceMap::operator=(const SourceMap&) -> SourceMap& = default;

auto SourceMap::operator=(SourceMap&&) noexcept -> SourceMap& = default;

SourceMap::~SourceMap() = default;

void SourceMap::add_source(Arc<const Source> source) {
  auto it = std::ranges::find_if(impl_->sources, [&](const auto& existing) {
    return existing->index == source->index;
  });
  if (it != impl_->sources.end()) {
    return;
  }
  impl_->sources.push_back(std::move(source));
}

void SourceMap::add_primary_source(Arc<const Source> source) {
  impl_->primary_source = source;
  add_source(std::move(source));
}

auto SourceMap::add_call_site(location call_site) -> CallSiteId {
  impl_->call_sites.push_back(call_site);
  return detail::narrow_cast<CallSiteId>(impl_->call_sites.size());
}

auto SourceMap::source(SourceId id) const -> Option<const Source&> {
  if (id == 0 and impl_->primary_source) {
    return **impl_->primary_source;
  }
  auto it = std::ranges::find_if(impl_->sources, [&](const auto& source) {
    return source->index == id;
  });
  if (it == impl_->sources.end()) {
    return None{};
  }
  return **it;
}

auto SourceMap::primary_source() const -> Option<const Source&> {
  if (not impl_->primary_source) {
    return None{};
  }
  return **impl_->primary_source;
}

auto SourceMap::translate(location loc) const -> location {
  if (not loc) {
    return loc;
  }
  auto result = loc;
  auto seen_call_sites = std::vector<CallSiteId>{};
  auto callsite = loc.callsite_index;
  while (callsite != 0) {
    if (std::ranges::contains(seen_call_sites, callsite)) {
      break;
    }
    seen_call_sites.push_back(callsite);
    auto call = call_site(callsite);
    if (not call) {
      break;
    }
    result = *call;
    callsite = call->callsite_index;
  }
  result.callsite_index = 0;
  if (result.source_index == 0) {
    return result;
  }
  if (impl_->primary_source
      and result.source_index == (*impl_->primary_source)->index) {
    result.source_index = 0;
  }
  return result;
}

auto SourceMap::call_site(CallSiteId id) const -> Option<location> {
  if (id < 1 or id > impl_->call_sites.size()) {
    return None{};
  }
  return impl_->call_sites[id - 1];
}

auto SourceMap::sources() const -> std::span<const Arc<const Source>> {
  return impl_->sources;
}

auto SourceMap::call_sites() const -> std::span<const location> {
  return impl_->call_sites;
}

} // namespace tenzir
