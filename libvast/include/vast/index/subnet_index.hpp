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
#include "vast/ewah_bitmap.hpp"
#include "vast/ids.hpp"
#include "vast/value_index.hpp"
#include "vast/view.hpp"

#include <caf/error.hpp>
#include <caf/expected.hpp>
#include <caf/fwd.hpp>

#include <array>
#include <cstdint>

namespace vast {

/// An index for subnets.
class subnet_index : public value_index {
public:
  using prefix_index = bitmap_index<uint8_t, equality_coder<ewah_bitmap>>;

  explicit subnet_index(vast::type t, caf::settings opts = {});

  caf::error inspect_impl(supported_inspectors& inspector) override;

  bool deserialize(detail::legacy_deserializer& source) override;

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

  value_index_ptr network_;
  prefix_index length_;
};

} // namespace vast
