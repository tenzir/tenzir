//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/location.hpp"

#include "tenzir/detail/narrow.hpp"
#include "tenzir/diagnostics.hpp"
#include "tenzir/source.hpp"

#include <algorithm>

namespace tenzir {

auto location::combine(into_location other) const -> location {
  if (not *this) {
    return other;
  }
  if (not other) {
    return *this;
  }
  if (source_index != other.source_index
      or callsite_index != other.callsite_index) {
    // The two locations originate from different sources, so their byte
    // offsets are not comparable and combining them would produce a
    // nonsensical span. AST nodes store their location at parse time, so this
    // case should not arise in practice; we keep the receiver's location as a
    // safety net rather than aborting.
    return *this;
  }
  return {std::min(begin, other.begin), std::max(end, other.end), source_index,
          callsite_index};
}

struct SourceMap::Impl {
  std::vector<Arc<const Source>> sources;
  std::vector<location> call_sites;
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

auto SourceMap::add_call_site(location call_site) -> CallSiteId {
  impl_->call_sites.push_back(call_site);
  return detail::narrow_cast<CallSiteId>(impl_->call_sites.size());
}

auto SourceMap::source(SourceId id) const -> Option<const Source&> {
  auto it = std::ranges::find_if(impl_->sources, [&](const auto& source) {
    return source->index == id;
  });
  if (it == impl_->sources.end()) {
    return None{};
  }
  return **it;
}

auto SourceMap::enrich(diagnostic diag) const -> diagnostic {
  auto callsite = CallSiteId{};
  for (const auto& annotation : diag.annotations) {
    if (annotation.primary and annotation.source) {
      callsite = annotation.source.callsite_index;
      break;
    }
  }
  if (callsite == 0) {
    return diag;
  }
  auto seen_call_sites = std::vector<CallSiteId>{};
  auto trace = std::vector<location>{};
  while (callsite != 0) {
    if (std::ranges::contains(seen_call_sites, callsite)) {
      break;
    }
    seen_call_sites.push_back(callsite);
    auto call = call_site(callsite);
    if (not call) {
      break;
    }
    trace.push_back(*call);
    callsite = call->callsite_index;
  }
  for (auto i = size_t{0}; i < trace.size(); ++i) {
    diag.annotations.emplace_back(trace[i].callsite_index == 0,
                                  "called from here", trace[i]);
  }
  return diag;
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
