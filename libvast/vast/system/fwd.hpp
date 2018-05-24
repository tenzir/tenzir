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

namespace vast::system {

// -- classes ------------------------------------------------------------------

class application;
class column_index;
class column_layout;
class configuration;
class default_application;
class export_command;
class import_command;
class indexer_manager;
class indexer_stage_driver;
class node_command;
class pcap_reader_command;
class pcap_writer_command;
class reader_command_base;
class remote_command;
class start_command;
class writer_command_base;

// -- structs ------------------------------------------------------------------

struct query_statistics;

// -- templates ----------------------------------------------------------------

template <class Reader>
class reader_command;

template <class Writer>
class writer_command;

// -- aliases ------------------------------------------------------------------

using column_index_ptr = std::unique_ptr<column_index>;

} // namespace vast::system
