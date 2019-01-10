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

#include <caf/fwd.hpp>

namespace vast::system {

// -- classes ------------------------------------------------------------------

class application;
class configuration;
class default_application;
class export_command;
class import_command;
class indexer_stage_driver;
class node_command;
class partition;
class pcap_reader_command;
class pcap_writer_command;
class remote_command;
class sink_command;
class source_command;
class start_command;
class table_indexer;

// -- structs ------------------------------------------------------------------

struct index_state;
struct measurement;
struct node_state;
struct query_status;
struct spawn_arguments;

// -- templates ----------------------------------------------------------------

template <class Reader>
class reader_command;

template <class Writer>
class writer_command;

// -- aliases ------------------------------------------------------------------

using atomic_measurement = std::atomic<measurement>;
using node_actor = caf::stateful_actor<node_state>;
using partition_ptr = std::unique_ptr<partition>;
using table_indexer_ptr = caf::intrusive_ptr<table_indexer>;


} // namespace vast::system
