//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2020 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <vast/as_bytes.hpp>
#include <vast/concept/printable/to_string.hpp>
#include <vast/concept/printable/vast/uuid.hpp>
#include <vast/detail/legacy_deserialize.hpp>
#include <vast/fbs/partition.hpp>
#include <vast/fbs/utils.hpp>
#include <vast/index/hash_index.hpp>
#include <vast/legacy_type.hpp>
#include <vast/qualified_record_field.hpp>
#include <vast/type.hpp>
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
    if (bound < idx.digests().size())
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

void print_partition_legacy(
  const vast::fbs::partition::LegacyPartition* partition, indentation& indent,
  const options& options) {
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
      vast::detail::legacy_deserializer bds(
        vast::as_bytes(ids_bytes->data(), ids_bytes->size()));
      if (!bds(restored_ids))
        std::cout << " (deserialization error)";
      else
        std::cout << rank(restored_ids);
      if (options.format.print_bytesizes)
        std::cout << " (" << print_bytesize(ids_bytes->size(), options.format)
                  << ")";
      std::cout << "\n";
    }
  }
  // Print catalog contents.
  std::cout << indent << "Catalog\n";
  if (auto partition_synopsis = partition->partition_synopsis()) {
    indented_scope _(indent);
    for (auto column_synopsis : *partition_synopsis->synopses()) {
      vast::qualified_record_field fqf;
      auto name = vast::fbs::deserialize_bytes(
        column_synopsis->qualified_record_field(), fqf);
      std::cout << indent << fqf.name() << ": ";
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
  // std::cout << indent << "Column Indices\n";
  vast::legacy_record_type intermediate;
  vast::fbs::deserialize_bytes(partition->combined_layout(), intermediate);
  auto combined_layout
    = caf::get<vast::record_type>(vast::type::from_legacy_type(intermediate));
  if (auto indexes = partition->indexes()) {
    if (indexes->size() != combined_layout.num_fields()) {
      std::cout << indent << "!! wrong number of fields\n";
      return;
    }
    const auto& expand_indexes = options.partition.expand_indexes;
    indented_scope _(indent);
    for (size_t i = 0; i < indexes->size(); ++i) {
      auto field = combined_layout.field(i);
      auto name = field.name;
      const auto* index = indexes->Get(i);
      if (!index) {
        std::cout << indent << "(missing index field " << name << ")\n";
        continue;
      }
      auto sz = index->index() && index->index()->data()
                  ? index->index()->data()->size()
                  : 0;
      std::cout << indent << name << ": " << fmt::to_string(field.type);
      if (options.format.print_bytesizes)
        std::cout << " (" << print_bytesize(sz, options.format) << ")";
      std::cout << "\n";
      bool expand
        = std::find(expand_indexes.begin(), expand_indexes.end(), name)
          != expand_indexes.end();
      if (expand && index->index()) {
        vast::factory_traits<vast::value_index>::initialize();
        vast::value_index_ptr state_ptr;
        if (auto error
            = vast::fbs::deserialize_bytes(index->index()->data(), state_ptr)) {
          std::cout << "!! failed to deserialize index" << to_string(error)
                    << std::endl;
          continue;
        }
        const auto& type = state_ptr->type();
        std::cout << indent << "- type: " << fmt::to_string(type) << std::endl;
        std::cout << indent << "- options: " << to_string(state_ptr->options())
                  << std::endl;
        // Print even more detailed information for hash indices.
        using namespace std::string_literals;
        if (auto index = type.attribute("index"))
          if (*index == "hash")
            print_hash_index(state_ptr, indent, options);
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
    case vast::fbs::partition::Partition::legacy:
      print_partition_legacy(partition->partition_as_legacy(), indent,
                             formatting);
      break;
    default:
      std::cout << "(unknown partition version)\n";
  }
}

} // namespace lsvast
