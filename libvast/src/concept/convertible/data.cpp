//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/concept/convertible/data.hpp"

namespace vast {

caf::expected<const data*>
get(const record& rec,
    const detail::stack_vector<const record_field*, 64>& trace) {
  const auto* src_section = &rec;
  auto it = src_section->end();
  size_t depth = 0;
  for (; depth < trace.size() - 1; depth++) {
    const auto* node = trace[depth];
    it = src_section->find(node->name);
    if (it == src_section->end())
      return nullptr;
    src_section = caf::get_if<record>(&it->second);
    if (!src_section)
      return caf::make_error(ec::convert_error, "{} is of unexpected type {}",
                             node->name, it->second);
  }
  it = src_section->find(trace[depth]->name);
  if (it == src_section->end())
    return nullptr;
  return &it->second;
}

} // namespace vast
