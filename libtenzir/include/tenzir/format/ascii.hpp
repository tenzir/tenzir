//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/defaults.hpp"
#include "tenzir/format/ostream_writer.hpp"

namespace tenzir::format::ascii {

class writer : public format::ostream_writer {
public:
  using super = format::ostream_writer;

  writer(ostream_ptr out, const caf::settings& options);

  caf::error write(const table_slice& x) override;

  [[nodiscard]] const char* name() const override;
};

} // namespace tenzir::format::ascii
