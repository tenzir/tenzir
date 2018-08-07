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

#include <caf/detail/comparable.hpp>
#include <caf/fwd.hpp>

#include "vast/fwd.hpp"
#include "vast/ptr_handle.hpp"

namespace vast {

/// Wraps a pointer to a table slize and makes it serializable.
class const_table_slice_handle
  : public ptr_handle<const table_slice>,
    caf::detail::comparable<const_table_slice_handle> {
public:
  // -- member types -----------------------------------------------------------

  using super = ptr_handle<const table_slice>;

  // -- constructors, destructors, and assignment operators --------------------

  using super::super;

  const_table_slice_handle(const table_slice_handle& other);

  ~const_table_slice_handle() override;
};

// -- related free functions ---------------------------------------------------

/// @relates table_slice_handle
caf::error inspect(caf::serializer& sink, const_table_slice_handle& hdl);

/// @relates table_slice_handle
caf::error inspect(caf::deserializer& source, const_table_slice_handle& hdl);

} // namespace vast
