/******************************************************************************
 *                    _   _____   __________                                  *
 *                   | | / / _ | / __/_  __/     Visibility                   *
 *                   | |/ / __ |_\ \  / /          Across                     *
 *                   |___/_/ |_/___/ /_/       Space and Time                 *
 *                                                                            *
 * This file is part of VAST. It is subject to the license terms in the       *
 * LICENSE file found in the top-level directory of this distribution and at  *
 * http://vast.io/license. No part of VAST, including this file, may be       *
 * copied, modified, propagated, or distributed except according to the terms *
 * contained in the LICENSE file.                                             *
 ******************************************************************************/

#pragma once

#include "vast/bitmap_index.hpp"
#include "vast/coder.hpp"
#include "vast/error.hpp"
#include "vast/ewah_bitmap.hpp"
#include "vast/ids.hpp"
#include "vast/value_index.hpp"
#include "vast/view.hpp"

#include <caf/error.hpp>
#include <caf/expected.hpp>
#include <caf/fwd.hpp>

namespace vast {

/// An index for transport-layer ports.
class port_index : public value_index {
public:
  using number_index
    = bitmap_index<port::number_type,
                   multi_level_coder<range_coder<ewah_bitmap>>>;

  using protocol_index
    = bitmap_index<std::underlying_type<port::port_type>::type,
                   equality_coder<ewah_bitmap>>;

  explicit port_index(vast::type t, caf::settings opts = {});

  caf::error serialize(caf::serializer& sink) const override;

  caf::error deserialize(caf::deserializer& source) override;

private:
  bool append_impl(data_view x, id pos) override;

  caf::expected<ids>
  lookup_impl(relational_operator op, data_view x) const override;

  number_index num_;
  protocol_index proto_;
};

} // namespace vast
