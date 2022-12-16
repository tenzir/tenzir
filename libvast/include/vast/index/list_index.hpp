//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2020 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/bitmap_index.hpp"
#include "vast/coder.hpp"
#include "vast/detail/legacy_deserialize.hpp"
#include "vast/error.hpp"
#include "vast/ids.hpp"
#include "vast/value_index.hpp"
#include "vast/view.hpp"

#include <caf/error.hpp>
#include <caf/expected.hpp>
#include <caf/fwd.hpp>

#include <cstdint>
#include <vector>

namespace vast {

/// An index for lists.
class list_index : public value_index {
public:
  /// Constructs a sequence index of a given type.
  /// @param t The sequence type.
  /// @param opts Runtime options for element type construction.
  explicit list_index(vast::type t, caf::settings opts = {});

  /// The bitmap index holding the sequence size.
  using size_bitmap_index
    = bitmap_index<uint32_t, multi_level_coder<range_coder<ids>>>;

  bool inspect_impl(supported_inspectors& inspector) override;

private:
  bool append_impl(data_view x, id pos) override;

  caf::expected<ids>
  lookup_impl(relational_operator op, data_view x) const override;

  size_t memusage_impl() const override;

  flatbuffers::Offset<fbs::ValueIndex>
  pack_impl(flatbuffers::FlatBufferBuilder& builder,
            flatbuffers::Offset<fbs::value_index::detail::ValueIndexBase>
              base_offset) override;

  caf::error unpack_impl(const fbs::ValueIndex& from) override;

  std::vector<value_index_ptr> elements_;
  size_t max_size_;
  size_bitmap_index size_;
  vast::type value_type_;
};

} // namespace vast
