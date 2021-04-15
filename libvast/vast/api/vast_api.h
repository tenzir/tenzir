//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <stddef.h>

#if VAST_ENABLE_ARROW
#  include "vast/api/arrow.h"
#endif

#define VAST_API __attribute__((visibility("default")))

#ifdef __cplusplus
extern "C" {
#endif

struct vast_info {
  const char* version;
};

struct VAST;

struct vast_connection;

// Returns static information about the local libvast.
VAST_API
int vast_info(struct vast_info* out);

// Initialize the actor system.
VAST_API
struct VAST* vast_initialize();

// Destroy the actor system.
VAST_API
void vast_finalize(struct VAST*);

// E.g.: `vast_open("localhost:42000")`
VAST_API
struct vast_connection* vast_open(struct VAST*, const char* endpoint);

// Not implemented yet.
// VAST_API
// int vast_close(struct VAST* vast, struct vast_connection* conn);

// Writes the output of `vast status` as a json string.
VAST_API
int vast_status_json(struct VAST*, struct vast_connection*, char* out,
                     size_t n);

#if 0
// TODO: Support for exporting arrow data.
VAST_API
int vast_export(struct vast_connection*, const char* query, struct ArrowSchema*, struct ArrowArray*);

VAST_API
int vast_export(struct vast_connection*, const char* query, char*);
#endif

#ifdef __cplusplus
}
#endif