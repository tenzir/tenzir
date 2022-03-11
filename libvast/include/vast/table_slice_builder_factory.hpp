//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2019 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/atoms.hpp"
#include "vast/factory.hpp"
#include "vast/table_slice_builder.hpp"
#include "vast/type.hpp"

namespace vast {

template <>
struct factory_traits<table_slice_builder> {
  using result_type = table_slice_builder_ptr;
  using key_type = table_slice_encoding;
  using signature = result_type (*)(type layout);

  static void initialize();

  template <class T>
  static key_type key() {
    return T::get_implementation_id();
  }

  template <class T>
  static result_type make(type layout) {
    return T::make(std::move(layout));
  }
};

} // namespace vast
