//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

// Signal handlers for SIGSEGV and SIGABRT are installed automatically at
// program startup via a constructor attribute.

// This function exists solely to ensure the object file is not discarded
// during static linking. It should be referenced from main() or another
// file that is guaranteed to be linked.
void signal_handlers_anchor();
