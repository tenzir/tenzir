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
#include "vast/ids.hpp"
#include "vast/value_index.hpp"
#include "vast/view.hpp"

#include <caf/error.hpp>
#include <caf/expected.hpp>
#include <caf/fwd.hpp>

namespace vast {

/// An index for floating-point values.
class real_index : public value_index {
public:
  /// Constructs a real index with an integral and fractional precisision,
  /// expressed as the number of digits to retain.
  explicit real_index(vast::type t, uint8_t integral_precision = 0,
                      uint8_t fractional_precision = 0);

  caf::error serialize(caf::serializer& sink) const override;

  caf::error deserialize(caf::deserializer& source) override;

private:
  std::pair<uint64_t, uint64_t> decompose(real x) const;

  bool append_impl(data_view x, id pos) override;

  caf::expected<ids>
  lookup_impl(relational_operator op, data_view x) const override;

  uint8_t integral_precision_;
  uint8_t fractional_precision_;
  ids sign_; // 0 = positive, 1 = negative
  ids zero_;
  ids nan_;
  ids inf_;
  bitmap_index<uint64_t, multi_level_coder<range_coder<ids>>> integral_;
  bitmap_index<uint64_t, multi_level_coder<range_coder<ids>>> fractional_;
};

} // namespace vast
