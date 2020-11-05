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

#include "vast/defaults.hpp"
#include "vast/format/ostream_writer.hpp"

namespace vast::format::ascii {

class writer : public format::ostream_writer {
public:
  using defaults = vast::defaults::export_::ascii;

  using super = format::ostream_writer;

  using super::super;

  caf::error write(const table_slice_ptr& x) override;

  const char* name() const override;
};

} // namespace vast::format::ascii
