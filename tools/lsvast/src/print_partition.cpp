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
#include <vast/system/passive_partition.hpp>
#include <vast/type.hpp>
#include <vast/value_index_factory.hpp>

#include <fmt/ranges.h>

#include <iostream>

#include "lsvast.hpp"
#include "util.hpp"

namespace lsvast {

template <size_t N>
void print_hash_index_(const vast::hash_index<N>& idx, indentation& indent,
                       const options& options) {
  fmt::print("{} - hash index bytes {}\n", indent, N);
  fmt::print("{} - {} digests\n", indent, idx.digests().size());
  indented_scope _(indent);
  if (options.format.verbosity == output_verbosity::normal) {
    const size_t bound = std::min<size_t>(idx.digests().size(), 3);
    for (size_t i = 0; i < bound; ++i) {
      // Workaround for fmt7.
      std::ostringstream ss;
      ss << idx.digests().at(i);
      fmt::print("{}{}\n", indent, ss.str());
    }
    if (bound < idx.digests().size())
      fmt::print("{}... (use -v to display remaining entries)\n", indent);
  } else {
    for (const auto& digest : idx.digests()) {
      // Workaround for fmt7.
      std::ostringstream ss;
      ss << digest;
      fmt::print("{}{}\n", indent, ss.str());
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
    fmt::print("more than 8 bytes digest :(\n");
  }
}

void print_partition_legacy(
  const vast::fbs::partition::LegacyPartition* partition, indentation& indent,
  const options& options) {
  if (!partition) {
    fmt::print("(null)\n");
    return;
  }
  fmt::print("{}Partition\n", indent);
  indented_scope _(indent);
  vast::uuid id;
  if (partition->uuid())
    if (auto error = unpack(*partition->uuid(), id))
      fmt::print(stderr, "{}{}", indent, to_string(error));
  fmt::print("{}uuid: {}\n", indent, id);
  fmt::print("{}offset: {}\n", indent, partition->offset());
  fmt::print("{}events: {}\n", indent, partition->events());
  // Print contained event types.
  fmt::print("{}Event Types: \n", indent);
  if (auto type_ids_vector = partition->type_ids()) {
    indented_scope _(indent);
    for (auto type_ids : *type_ids_vector) {
      auto name = type_ids->name()->c_str();
      auto ids_bytes = type_ids->ids();
      fmt::print("{}{}: ", indent, name);
      vast::ids restored_ids;
      vast::detail::legacy_deserializer bds(
        vast::as_bytes(ids_bytes->data(), ids_bytes->size()));
      if (!bds(restored_ids))
        fmt::print(" (deserialization error)");
      else
        fmt::print("{}", rank(restored_ids));
      if (options.format.print_bytesizes)
        fmt::print(" ({})", print_bytesize(ids_bytes->size(), options.format));
      fmt::print("\n");
    }
  }
  // Print catalog contents.
  fmt::print("{}Catalog\n", indent);
  if (auto partition_synopsis = partition->partition_synopsis()) {
    indented_scope _(indent);
    for (auto column_synopsis : *partition_synopsis->synopses()) {
      vast::qualified_record_field fqf;
      auto name = vast::fbs::deserialize_bytes(
        column_synopsis->qualified_record_field(), fqf);
      fmt::print("{}{}: ", indent, fqf.name());
      if (auto opaque = column_synopsis->opaque_synopsis()) {
        fmt::print("opaque_synopsis");
        if (options.format.print_bytesizes)
          fmt::print(" ({})",
                     print_bytesize(opaque->data()->size(), options.format));
      } else if (auto bs = column_synopsis->bool_synopsis()) {
        fmt::print("bool_synopis {} {}", bs->any_true(), bs->any_false());
      } else if (auto ts = column_synopsis->time_synopsis()) {
        fmt::print("time_synopsis {}-{}", ts->start(), ts->end());
      } else {
        fmt::print("(unknown)");
      }
      fmt::print("\n");
    }
  }
  // Print column indices.
  fmt::print("{}Column Indexes\n", indent);
  vast::legacy_record_type intermediate;
  auto err
    = vast::fbs::deserialize_bytes(partition->combined_layout(), intermediate);
  if (err)
    fmt::print(stderr, "Err during combined layout deserialization {}", err);
  auto combined_layout
    = caf::get<vast::record_type>(vast::type::from_legacy_type(intermediate));
  if (auto const* indexes = partition->indexes()) {
    if (indexes->size() != combined_layout.num_fields()) {
      fmt::print("{}!! wrong number of fields\n", indent);
      return;
    }
    auto const& expand_indexes = options.partition.expand_indexes;
    indented_scope _(indent);
    for (size_t i = 0; i < indexes->size(); ++i) {
      auto field = combined_layout.field(i);
      auto name = field.name;
      auto const* index = indexes->Get(i);
      if (!index) {
        fmt::print("{}(missing index field {})\n", indent, name);
        continue;
      }
      fmt::print("{}{}: {}", indent, name, field.type);
      auto const* legacy_index = index->index();
      if (!legacy_index) {
        fmt::print(" (no legacy_index)\n");
        continue;
      }
      if (options.format.print_bytesizes) {
        auto size_string = std::string{};
        if (!legacy_index->data())
          size_string = "null";
        else
          size_string
            = print_bytesize(legacy_index->data()->size(), options.format);
        if (legacy_index->external_container_idx() > 0) {
          if (legacy_index->data())
            fmt::print("!! index {} has both inline and external data\n", name);
          size_string = fmt::format("in external chunk {}",
                                    legacy_index->external_container_idx());
        } else
          size_string += " inline";

        fmt::print(" ({})", size_string);
      }
      fmt::print("\n");
      bool expand
        = std::find(expand_indexes.begin(), expand_indexes.end(), name)
          != expand_indexes.end();
      if (expand) {
        vast::factory_traits<vast::value_index>::initialize();
        vast::value_index_ptr state_ptr;
        if (auto error
            = vast::fbs::deserialize_bytes(index->index()->data(), state_ptr)) {
          fmt::print("!! failed to deserialize index: {}\n", error);
          continue;
        }
        const auto& type = state_ptr->type();
        fmt::print("{}- type: {}\n", indent, type);
        fmt::print("{}- options: {}\n", indent, state_ptr->options());
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
  auto chunk = vast::chunk::mmap(path);
  if (!chunk) {
    fmt::print("(failed to open file: {})\n", chunk.error());
    return;
  }
  if (flatbuffers::BufferHasIdentifier(
        (*chunk)->data(), vast::fbs::SegmentedFileHeaderIdentifier())
      && formatting.partition.print_header) {
    auto const* header = vast::fbs::GetSegmentedFileHeader((*chunk)->data());
    print_segmented_file_header(header, indent, formatting);
  }
  auto maybe_partition = vast::system::partition_chunk::get_flatbuffer(*chunk);
  if (!maybe_partition) {
    fmt::print("(failed to read partition: {}\n", maybe_partition.error());
    return;
  }
  auto& partition = *maybe_partition;
  switch (partition->partition_type()) {
    case vast::fbs::partition::Partition::legacy:
      print_partition_legacy(partition->partition_as_legacy(), indent,
                             formatting);
      break;
    default:
      fmt::print("(unknown partition version)\n");
  }
}

} // namespace lsvast
