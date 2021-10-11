//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2020 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <vast/concept/printable/to_string.hpp>
#include <vast/concept/printable/vast/legacy_type.hpp>
#include <vast/concept/printable/vast/uuid.hpp>
#include <vast/fbs/partition.hpp>
#include <vast/fbs/utils.hpp>
#include <vast/index/hash_index.hpp>
#include <vast/legacy_qualified_record_field.hpp>
#include <vast/value_index_factory.hpp>

#include <iostream>

#include "lsvast.hpp"
#include "util.hpp"

namespace lsvast {

template <size_t N>
void print_hash_index_(const vast::hash_index<N>& idx, indentation& indent,
                       const options& options) {
  std::cout << indent << " - hash index bytes " << N << "\n";
  std::cout << indent << " - " << idx.digests().size() << " digests:"
            << "\n";
  indented_scope _(indent);
  if (options.format.verbosity == output_verbosity::normal) {
    const size_t bound = std::min<size_t>(idx.digests().size(), 3);
    for (size_t i = 0; i < bound; ++i) {
      std::cout << indent << idx.digests().at(i) << "\n";
    }
    std::cout << indent << "... (use -v to display remaining entries)\n";
  } else {
    for (const auto& digest : idx.digests()) {
      std::cout << indent << digest << "\n";
    }
  }
}

void print_hash_index(const vast::value_index_ptr& ptr, indentation& indent,
                      const options& options) {
  // TODO: This is probably a good use case for a macro.
  if (auto idx = dynamic_cast<const vast::hash_index<1>*>(ptr.get())) {
    print_hash_index_(*idx, indent, options);
  } else if (auto idx = dynamic_cast<const vast::hash_index<1>*>(ptr.get())) {
    print_hash_index_(*idx, indent, options);
  } else if (auto idx = dynamic_cast<const vast::hash_index<2>*>(ptr.get())) {
    print_hash_index_(*idx, indent, options);
  } else if (auto idx = dynamic_cast<const vast::hash_index<3>*>(ptr.get())) {
    print_hash_index_(*idx, indent, options);
  } else if (auto idx = dynamic_cast<const vast::hash_index<4>*>(ptr.get())) {
    print_hash_index_(*idx, indent, options);
  } else if (auto idx = dynamic_cast<const vast::hash_index<5>*>(ptr.get())) {
    print_hash_index_(*idx, indent, options);
  } else if (auto idx = dynamic_cast<const vast::hash_index<6>*>(ptr.get())) {
    print_hash_index_(*idx, indent, options);
  } else if (auto idx = dynamic_cast<const vast::hash_index<7>*>(ptr.get())) {
    print_hash_index_(*idx, indent, options);
  } else if (auto idx = dynamic_cast<const vast::hash_index<8>*>(ptr.get())) {
    print_hash_index_(*idx, indent, options);
  } else {
    std::cout << "more than 8 bytes digest :(\n";
  }
}

void print_partition_v0(const vast::fbs::partition::v0* partition,
                        indentation& indent, const options& options) {
  if (!partition) {
    std::cout << "(null)\n";
    return;
  }
  std::cout << indent << "Partition\n";
  indented_scope _(indent);
  vast::uuid id;
  if (partition->uuid())
    if (auto error = unpack(*partition->uuid(), id))
      std::cerr << indent << to_string(error);
  std::cout << indent << "uuid: " << to_string(id) << "\n";
  std::cout << indent << "offset: " << partition->offset() << "\n";
  std::cout << indent << "events: " << partition->events() << "\n";
  // Print contained event types.
  std::cout << indent << "Event Types: \n";
  if (auto type_ids_vector = partition->type_ids()) {
    indented_scope _(indent);
    for (auto type_ids : *type_ids_vector) {
      auto name = type_ids->name()->c_str();
      auto ids_bytes = type_ids->ids();
      std::cout << indent << name << ": ";
      vast::ids restored_ids;
      caf::binary_deserializer bds(
        nullptr, reinterpret_cast<const char*>(ids_bytes->data()),
        ids_bytes->size());
      if (auto error = bds(restored_ids))
        std::cout << " (error: " << caf::to_string(error) << ")";
      else
        std::cout << rank(restored_ids);
      if (options.format.print_bytesizes)
        std::cout << " (" << print_bytesize(ids_bytes->size(), options.format)
                  << ")";
      std::cout << "\n";
    }
  }
  // Print meta index contents.
  std::cout << indent << "Meta Index\n";
  if (auto partition_synopsis = partition->partition_synopsis()) {
    indented_scope _(indent);
    for (auto column_synopsis : *partition_synopsis->synopses()) {
      vast::legacy_qualified_record_field fqf;
      auto name = vast::fbs::deserialize_bytes(
        column_synopsis->legacy_qualified_record_field(), fqf);
      std::cout << indent << fqf.fqn() << ": ";
      if (auto opaque = column_synopsis->opaque_synopsis()) {
        std::cout << "opaque_synopsis";
        if (options.format.print_bytesizes)
          std::cout << " ("
                    << print_bytesize(opaque->data()->size(), options.format)
                    << ")";
      } else if (auto bs = column_synopsis->bool_synopsis()) {
        std::cout << "bool_synopis " << bs->any_true() << " "
                  << bs->any_false();
      } else if (auto ts = column_synopsis->time_synopsis()) {
        std::cout << "time_synopsis " << ts->start() << "-" << ts->end();
      } else {
        std::cout << "(unknown)";
      }
      std::cout << '\n';
    }
  }
  // Print column indices.
  std::cout << indent << "Column Indices\n";
  vast::legacy_record_type combined_layout;
  vast::fbs::deserialize_bytes(partition->combined_layout(), combined_layout);
  if (auto indexes = partition->indexes()) {
    if (indexes->size() != combined_layout.fields.size()) {
      std::cout << indent << "!! wrong number of fields\n";
      return;
    }
    const auto& expand_indexes = options.partition.expand_indexes;
    indented_scope _(indent);
    for (size_t i = 0; i < indexes->size(); ++i) {
      auto field = combined_layout.fields.at(i);
      const auto* index = indexes->Get(i);
      auto name = field.name;
      auto sz = index->index()->data()->size();
      std::cout << indent << name << ": " << vast::to_string(field.type);
      if (options.format.print_bytesizes)
        std::cout << " (" << print_bytesize(sz, options.format) << ")";
      std::cout << "\n";
      bool expand
        = std::find(expand_indexes.begin(), expand_indexes.end(), name)
          != expand_indexes.end();
      if (expand) {
        vast::factory_traits<vast::value_index>::initialize();
        vast::value_index_ptr state_ptr;
        if (auto error
            = vast::fbs::deserialize_bytes(index->index()->data(), state_ptr)) {
          std::cout << "!! failed to deserialize index" << to_string(error)
                    << std::endl;
          continue;
        }
        const auto& type = state_ptr->type();
        std::cout << indent << "- type: " << to_string(type) << std::endl;
        std::cout << indent << "- options: " << to_string(state_ptr->options())
                  << std::endl;
        // Print even more detailed information for hash indices.
        using namespace std::string_literals;
        if (std::any_of(type.attributes().begin(), type.attributes().end(),
                        [](const vast::attribute& x) {
                          return x.key == "index" && x.value == "hash"s;
                        })) {
          print_hash_index(state_ptr, indent, options);
        }
      }
    }
  }
}

void print_partition(const std::filesystem::path& path, indentation& indent,
                     const options& formatting) {
  auto partition = read_flatbuffer_file<vast::fbs::Partition>(path);
  if (!partition) {
    std::cout << "(error reading partition file " << path.string() << ")\n";
    return;
  }
  switch (partition->partition_type()) {
    case vast::fbs::partition::Partition::v0:
      print_partition_v0(partition->partition_as_v0(), indent, formatting);
      break;
    default:
      std::cout << "(unknown partition version)\n";
  }
}

} // namespace lsvast
