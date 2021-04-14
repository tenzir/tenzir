//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

struct vast_info {
  const char* version;
};

// struct vast_event_statistics {
// 	const char* name;
// 	size_t count;
// };

// struct vast_contents {
//   size_t n;
//   struct vast_event_statistics counts[0];
// };

struct VAST;

// E.g.: `vast_open("localhost:42000")`
struct VAST* vast_open(const char* endpoint);

// Returns information about the local libvast.
int vast_info(struct vast_info* out);

// Writes the
int vast_status_json(struct VAST*, char* out, size_t n);

// Closes the connection
void vast_close(struct VAST*);

#ifdef __cplusplus
}
#endif