// SPDX-FileCopyrightText: (c) 2020 Tenzir GmbH <info@tenzir.com>
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/factory.hpp"
#include "vast/format/writer.hpp"

#include <string>

namespace vast {

template <>
struct factory_traits<format::writer> {
  using result_type = caf::expected<format::writer_ptr>;
  using key_type = std::string;
  using signature = result_type (*)(const caf::settings&);

  static void initialize();
};

} // namespace vast
