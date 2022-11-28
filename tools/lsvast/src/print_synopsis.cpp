//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <vast/bloom_filter_synopsis.hpp>
#include <vast/fbs/synopsis.hpp>
#include <vast/fbs/utils.hpp>
#include <vast/qualified_record_field.hpp>
#include <vast/synopsis.hpp>

#include "lsvast.hpp"
#include "util.hpp"

namespace lsvast {

template <typename Type, typename HashFunction>
void print_raw_bloom_filter(
  const vast::bloom_filter_synopsis<Type, HashFunction>* bloom_synopsis,
  indentation& indent) {
  auto const& filter = bloom_synopsis->filter();
  auto const& data = filter.data();
  auto idx = 0ull;
  auto it = data.begin();
  unsigned char value = 0;
  fmt::print("\n");
  auto scope = indented_scope{indent};
  while (idx < data.size()) {
    value = (value << 1u) | *it++;
    if (idx % 256 == 0)
      fmt::print("{}", indent);
    ++idx;
    if (idx % 8 == 0)
      fmt::print("{:02x}", value);
    if (idx % 256 == 0)
      fmt::print("\n");
    else if (idx % 16 == 0)
      fmt::print(" ");
  }
  auto remainder = idx % 8;
  if (remainder > 0) {
    while (idx % 8 != 0) {
      value = value << 1u;
      ++idx;
    }
    fmt::print("{:02x}", remainder);
  }
  fmt::print("\n");
}

void print_synopsis(const vast::fbs::synopsis::LegacySynopsis* synopsis,
                    indentation& indent, const options& options) {
  if (!synopsis) {
    fmt::print("(null)\n");
    return;
  }
  vast::qualified_record_field fqf;
  auto name
    = vast::fbs::deserialize_bytes(synopsis->qualified_record_field(), fqf);
  if (!fqf.name().empty())
    fmt::print("{}field {}: ", indent, fqf.name());
  else
    fmt::print("{}type {}: ", indent, fqf.type());
  if (auto const* ts = synopsis->time_synopsis())
    fmt::print("time_synopsis: {}-{}\n", ts->start(), ts->end());
  else if (auto const* bs = synopsis->bool_synopsis())
    fmt::print("bool_synopsis: {} {}\n", bs->any_true(), bs->any_false());
  else if (auto const* os = synopsis->opaque_synopsis()) {
    fmt::print("opaque_synopsis");
    const auto size = os->caf_0_17_data() ? os->caf_0_17_data()->size()
                                          : os->caf_0_18_data()->size();
    if (options.format.print_bytesizes)
      fmt::print(" ({})", print_bytesize(size, options.format));
    if (options.synopsis.bloom_raw) {
      auto ptr = vast::synopsis_ptr{};
      if (auto error = unpack(*synopsis, ptr)) {
        return;
      }
      // Try the two options that are currently being produced by VAST.
      using string_synopsis
        = vast::bloom_filter_synopsis<std::string, vast::legacy_hash>;
      using address_synopsis
        = vast::bloom_filter_synopsis<vast::address, vast::legacy_hash>;
      if (auto const* bloom_synopsis
          = dynamic_cast<string_synopsis*>(ptr.get())) {
        print_raw_bloom_filter(bloom_synopsis, indent);
      } else if (auto const* bloom_synopsis
                 = dynamic_cast<address_synopsis*>(ptr.get())) {
        print_raw_bloom_filter(bloom_synopsis, indent);
      } else {
        fmt::print("(unknown bloom filter type)\n");
      }
    }
    fmt::print("\n");
  } else
    fmt::print("(unknown)\n");
}

} // namespace lsvast
