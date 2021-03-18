// SPDX-FileCopyrightText: (c) 2019 Tenzir GmbH <info@tenzir.com>
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <caf/optional.hpp>

#include "vast/schema.hpp"

namespace vast::event_types {

/// Initializes the system-wide type registry.
/// @param s The schema.
/// @returns true on success or false if registry was alread initialized.
bool init(schema s);

/// Retrieves a pointer to the system-wide type registry.
/// @returns nullptr if registry is not initialized.
const schema* get();

} // namespace vast::event_types
