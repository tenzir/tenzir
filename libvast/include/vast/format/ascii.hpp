//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/defaults.hpp"
#include "vast/format/ostream_writer.hpp"

namespace vast::format::ascii {

class writer : public format::ostream_writer {
public:
  using super = format::ostream_writer;

  writer(ostream_ptr out, const caf::settings& options);

  caf::error write(const table_slice& x) override;

  [[nodiscard]] const char* name() const override;
};

} // namespace vast::format::ascii
