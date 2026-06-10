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

namespace tenzir {

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

auto SourceMap::add_source(Arc<const Source> source) -> SourceId {
  auto result = source->index;
  impl_->sources.push_back(std::move(source));
  return result;
}

auto SourceMap::add_call_site(location call_site) -> CallSiteId {
  impl_->call_sites.push_back(call_site);
  return detail::narrow_cast<CallSiteId>(impl_->call_sites.size());
}

auto SourceMap::source(SourceId id) const -> const Source& {
  auto it = std::ranges::find_if(impl_->sources, [&](const auto& source) {
    return source->index == id;
  });
  TENZIR_ASSERT(it != impl_->sources.end());
  return **it;
}

auto SourceMap::call_site(CallSiteId id) const -> location {
  TENZIR_ASSERT(id >= 1);
  TENZIR_ASSERT(id <= impl_->call_sites.size());
  return impl_->call_sites[id - 1];
}

auto SourceMap::sources() const -> std::span<const Arc<const Source>> {
  return impl_->sources;
}

auto SourceMap::call_sites() const -> std::span<const location> {
  return impl_->call_sites;
}

} // namespace tenzir
