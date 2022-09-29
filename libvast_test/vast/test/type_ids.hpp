//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/view.hpp"

#include <caf/type_id.hpp>

#include <string_view>

namespace vast {
template <class T>
class container_view_handle;
}

CAF_BEGIN_TYPE_ID_BLOCK(vast_ut_block, 50000)
  CAF_ADD_TYPE_ID(vast_ut_block, (vast::pattern_view))
  CAF_ADD_TYPE_ID(vast_ut_block,
                  (vast::container_view_handle<vast::list_view_ptr>))
  CAF_ADD_TYPE_ID(vast_ut_block,
                  (vast::container_view_handle<vast::map_view_ptr>))
  CAF_ADD_TYPE_ID(vast_ut_block,
                  (vast::container_view_handle<vast::record_view_ptr>))
  CAF_ADD_TYPE_ID(vast_ut_block, (std::string_view))
CAF_END_TYPE_ID_BLOCK(vast_ut_block)
