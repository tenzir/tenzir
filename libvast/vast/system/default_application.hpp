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

#include <memory>
#include <string>
#include <string_view>

#include "vast/command.hpp"

#include "vast/system/application.hpp"
#include "vast/system/export_command.hpp"
#include "vast/system/import_command.hpp"

namespace vast::system {

class default_application : public application {
public:
  default_application();

  import_command& import_cmd() {
    VAST_ASSERT(import_ != nullptr);
    return *import_;
  }

  export_command& export_cmd() {
    VAST_ASSERT(export_ != nullptr);
    return *export_;
  }

private:
  import_command* import_;
  export_command* export_;
};

} // namespace vast::system
