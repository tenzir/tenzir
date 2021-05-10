//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2020 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <vast/chunk.hpp>
#include <vast/concept/printable/to_string.hpp>
#include <vast/concept/printable/vast/type.hpp>
#include <vast/concept/printable/vast/uuid.hpp>
#include <vast/error.hpp>
#include <vast/fbs/index.hpp>
#include <vast/fbs/partition.hpp>
#include <vast/fbs/segment.hpp>
#include <vast/fbs/utils.hpp>
#include <vast/ids.hpp>
#include <vast/io/read.hpp>
#include <vast/qualified_record_field.hpp>
#include <vast/table_slice.hpp>
#include <vast/type.hpp>
#include <vast/uuid.hpp>

#include <caf/binary_deserializer.hpp>
#include <caf/error.hpp>
#include <flatbuffers/flatbuffers.h>

#include <cstddef>
#include <filesystem>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <string>
#include <system_error>

// clang-format off
// TODO: Implement different output formats for human-readable and
// machine-readable output. For example:
//
// $ lsvast vast.db/index/index.bin
// Index Flatbuffer (v0)
// Events:
//   vast.metrics: 2
//   suricata.dns: 201
// Partitions:
//   2fc1fde8-5705-455b-8404-967493bd9219
//   477769c6-8e9a-46d5-bee8-0086924a150a
//
//
// $ lsvast --json vast.db/index/index.bin
// {
//   "filename": "vast.db/index/index.bin",
//   "type": "Index",
//   "contents": {
//     "index": {
//       "v0": {
//         "partitions": [
//           {
//             "data": "\x2f\xc1\xfd\xe8-\x57\x05-\x45\x5b-\x84\x04-\x96\x74\x93\xbd\x92\x19"
//           },
//           {
//             "data": "\x47\x77\x69\xc6-\x8e\x9a-\x46\xd5-\xbe\xe8-\x00\x86\x92\x4a\x15\x0a"
//           }
//         ]
//         "stats": [
//           {
//             "name": "vast.metrics",
//             "count": 2
//           },
//           {
//             "name": "suricata.dns",
//             "count": 201
//           }
//         ]
//         }
//       }
//     }
//   }
// }
//
// clang-format on

using std::cerr;
using std::cout;
using std::endl;

using namespace std::string_literals;

enum class Kind {
  Unknown,
  DatabaseDir,
  Partition,
  Index,
  Segment,
};

enum class output_verbosity : uint8_t {
  normal,
  verbose,
};

struct formatting_options {
  output_verbosity verbosity = output_verbosity::normal;
  bool print_bytesizes = false;
  // Print e.g. "2 TiB" instead of '2199023255552'.
  bool human_readable_numbers = false;
};

struct indentation;

typedef void (*printer)(const std::filesystem::path&, indentation&,
                        const formatting_options&);

void print_unknown(const std::filesystem::path&, indentation&,
                   const formatting_options&);
void print_vast_db(const std::filesystem::path&, indentation&,
                   const formatting_options&);
void print_partition(const std::filesystem::path&, indentation&,
                     const formatting_options&);
void print_index(const std::filesystem::path&, indentation&,
                 const formatting_options&);
void print_segment(const std::filesystem::path&, indentation&,
                   const formatting_options&);

static const std::map<Kind, printer> printers = {
  {Kind::Unknown, print_unknown}, {Kind::DatabaseDir, print_vast_db},
  {Kind::Index, print_index},     {Kind::Partition, print_partition},
  {Kind::Segment, print_segment},
};

caf::expected<Kind> classify(const std::filesystem::path& path) {
  std::error_code err{};
  const auto is_directory = std::filesystem::is_directory(path, err);
  if (err)
    return caf::make_error(vast::ec::filesystem_error,
                           "Invalid path: " + path.string(), err.message());
  if (is_directory) {
    auto version_file = path / "VERSION";
    auto is_regular_file = std::filesystem::is_regular_file(version_file, err);
    if (err)
      return caf::make_error(vast::ec::filesystem_error,
                             "Invalid path: " + version_file.string(),
                             err.message());
    if (is_regular_file)
      return Kind::DatabaseDir;
  }
  const auto is_regular_file = std::filesystem::is_regular_file(path, err);
  if (err)
    return caf::make_error(vast::ec::filesystem_error,
                           "Not a file: ", err.message());

  if (!is_regular_file)
    return Kind::Unknown;
  auto bytes = vast::io::read(path);
  if (!bytes)
    return Kind::Unknown;
  auto buf = bytes->data();
  if (vast::fbs::IndexBufferHasIdentifier(buf))
    return Kind::Index;
  if (vast::fbs::PartitionBufferHasIdentifier(buf))
    return Kind::Partition;
  if (vast::fbs::SegmentBufferHasIdentifier(buf))
    return Kind::Segment;
  return Kind::Unknown;
}

template <typename T>
struct flatbuffer_deleter {
  // Plumbing for a move-only type.
  flatbuffer_deleter() = default;
  flatbuffer_deleter(const flatbuffer_deleter&) = delete;
  flatbuffer_deleter(flatbuffer_deleter&&) = default;

  flatbuffer_deleter(std::vector<std::byte>&& c) : chunk_(std::move(c)) {
  }

  void operator()(const T*) {
    // nop (the destructor of `chunk_` already releases the memory)
  }

  std::vector<std::byte> chunk_;
};

// Get contents of the specified file as versioned flatbuffer, or nullptr in
// case of a read error/version mismatch.
// The unique_pointer is used to have a pointer with the correct flatbuffer
// type, that will still delete the underlying vector from `io::read`
// automatically upon destruction.
template <typename T>
std::unique_ptr<const T, flatbuffer_deleter<T>>
read_flatbuffer_file(const std::filesystem::path& path) {
  using result_t = std::unique_ptr<const T, flatbuffer_deleter<T>>;
  auto result
    = result_t(static_cast<const T*>(nullptr), flatbuffer_deleter<T>{});
  auto maybe_bytes = vast::io::read(path);
  if (!maybe_bytes)
    return result;
  auto bytes = std::move(*maybe_bytes);
  flatbuffers::Verifier verifier{reinterpret_cast<const uint8_t*>(bytes.data()),
                                 bytes.size()};
  if (!verifier.template VerifyBuffer<T>())
    return result;
  const auto* ptr = flatbuffers::GetRoot<T>(bytes.data());
  return result_t(ptr, flatbuffer_deleter<T>(std::move(bytes)));
}

struct indentation {
public:
  static constexpr const int TAB_WIDTH = 2;
  indentation() {
  }
  void increase(int level = TAB_WIDTH) {
    levels_.push_back(level);
  }
  void decrease() {
    levels_.pop_back();
  }

  friend std::ostream& operator<<(std::ostream&, const indentation&);

private:
  std::vector<int> levels_;
};

class indented_scope {
public:
  indented_scope(indentation& indent) : indent_(indent) {
    indent.increase();
  }
  ~indented_scope() {
    indent_.decrease();
  }

private:
  indentation& indent_;
};

std::ostream& operator<<(std::ostream& str, const indentation& indent) {
  for (auto level : indent.levels_)
    for (int i = 0; i < level; ++i)
      str << ' ';
  return str;
}

std::ostream& operator<<(std::ostream& out, const vast::fbs::uuid::v0* uuid) {
  if (!uuid || !uuid->data())
    return out << "(null)";
  auto old_flags = out.flags();
  for (size_t i = 0; i < uuid->data()->size(); ++i)
    out << std::hex << +uuid->data()->Get(i);
  out.flags(old_flags); // `std::hex` is sticky.
  return out;
}

void print_unknown(const std::filesystem::path& path, indentation& indent,
                   const formatting_options&) {
  std::cout << indent << "(unknown " << path.string() << ")\n";
}

std::string print_bytesize(size_t bytes, const formatting_options& formatting) {
  const char* suffixes[] = {
    " B", " KiB", " MiB", " GiB", " TiB", " EiB",
  };
  std::stringstream ss;
  if (!formatting.human_readable_numbers) {
    ss << bytes;
  } else {
    size_t idx = 0;
    double fbytes = bytes;
    while (fbytes > 1024 && idx < std::size(suffixes)) {
      ++idx;
      fbytes /= 1024;
    }
    // Special case to avoid weird output like `34.0 B`.
    if (idx == 0)
      ss << bytes;
    else
      ss << std::fixed << std::setprecision(1) << fbytes;
    ss << suffixes[idx];
  }
  return std::move(ss).str();
}

void print_vast_db(const std::filesystem::path& vast_db, indentation& indent,
                   const formatting_options& formatting) {
  // TODO: We should have some versioning for the layout
  // of the vast.db directory itself, so we can still read
  // older versions.
  auto index_dir = vast_db / "index";
  std::cout << indent << index_dir.string() << "/\n";
  {
    indented_scope _(indent);
    std::cout << indent << "index.bin - ";
    print_index(index_dir / "index.bin", indent, formatting);
    std::error_code err{};
    auto dir = std::filesystem::directory_iterator{index_dir, err};
    if (err) {
      std::cerr << "Failed to find vast db index directory: " + err.message()
                << std::endl;
    } else {
      for (const auto& entry : dir) {
        const auto stem = entry.path().stem();
        if (stem == "index")
          continue;
        const auto extension = entry.path().extension();
        // TODO: Print partition synopses.
        if (extension == ".mdx")
          continue;
        std::cout << indent << stem << " - ";
        print_partition(entry.path(), indent, formatting);
      }
    }
  }
  const auto segments_dir = vast_db / "archive" / "segments";
  std::cout << indent << segments_dir.string() << "/\n";
  {
    indented_scope _(indent);
    std::error_code err{};
    auto dir = std::filesystem::directory_iterator{segments_dir, err};
    if (err) {
      std::cerr << "Failed to find vast db segments directory: " + err.message()
                << std::endl;
    } else {
      for (const auto& entry : dir) {
        const auto stem = entry.path().stem();
        std::cout << indent << stem << " - ";
        print_segment(entry.path(), indent, formatting);
      }
    }
  }
}

void print_partition_v0(const vast::fbs::partition::v0* partition,
                        indentation& indent,
                        const formatting_options& formatting) {
  if (!partition) {
    std::cout << "(null)\n";
    return;
  }
  std::cout << indent << "Partition\n";
  indented_scope _(indent);
  vast::uuid id;
  if (partition->uuid())
    unpack(*partition->uuid(), id);
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
      if (formatting.print_bytesizes)
        std::cout << " (" << print_bytesize(ids_bytes->size(), formatting)
                  << ")";
      std::cout << "\n";
    }
  }
  // Print meta index contents.
  std::cout << indent << "Meta Index\n";
  if (auto partition_synopsis = partition->partition_synopsis()) {
    indented_scope _(indent);
    for (auto column_synopsis : *partition_synopsis->synopses()) {
      vast::qualified_record_field fqf;
      auto name = vast::fbs::deserialize_bytes(
        column_synopsis->qualified_record_field(), fqf);
      std::cout << indent << fqf.fqn() << ": ";
      if (auto opaque = column_synopsis->opaque_synopsis()) {
        std::cout << "opaque_synopsis";
        if (formatting.print_bytesizes)
          std::cout << " ("
                    << print_bytesize(opaque->data()->size(), formatting)
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
  vast::record_type combined_layout;
  vast::fbs::deserialize_bytes(partition->combined_layout(), combined_layout);
  if (auto indexes = partition->indexes()) {
    if (indexes->size() != combined_layout.fields.size()) {
      std::cout << indent << "weird :/\n";
      return;
    }
    indented_scope _(indent);
    for (size_t i = 0; i < indexes->size(); ++i) {
      auto field = combined_layout.fields.at(i);
      auto index = indexes->Get(i);
      auto name = field.name;
      // auto name = index->qualified_field_name();
      auto sz = index->index()->data()->size();
      std::cout << indent << name << ": " << vast::to_string(field.type);
      if (formatting.print_bytesizes)
        std::cout << " (" << print_bytesize(sz, formatting) << ")";
      std::cout << "\n";
    }
  }
}

void print_partition(const std::filesystem::path& path, indentation& indent,
                     const formatting_options& formatting) {
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

void print_index_v0(const vast::fbs::index::v0* index, indentation& indent,
                    const formatting_options&) {
  if (!index) {
    std::cout << "(null)\n";
    return;
  }
  std::cout << indent << "Index\n";
  indented_scope _(indent);
  // print layouts
  std::cout << indent << "layouts:\n";
  if (auto stats = index->stats()) {
    indented_scope _(indent);
    for (auto stat : *stats)
      std::cout << indent << stat->name()->c_str() << ": " << stat->count()
                << std::endl;
  }
  // print partitions
  std::cout << indent << "partitions: ";
  if (auto partitions = index->partitions()) {
    std::cout << '[';
    for (size_t i = 0; i < partitions->size(); ++i) {
      auto uuid = partitions->Get(i);
      std::cout << uuid;
      if (i < partitions->size() - 1)
        std::cout << ", ";
    }
    std::cout << ']';
  }
  std::cout << "\n";
}

void print_index(const std::filesystem::path& path, indentation& indent,
                 const formatting_options& formatting) {
  auto index = read_flatbuffer_file<vast::fbs::Index>(path);
  if (!index) {
    std::cout << indent << "(error reading index file " << path.string()
              << ")\n";
  }
  switch (index->index_type()) {
    case vast::fbs::index::Index::v0:
      print_index_v0(index->index_as_v0(), indent, formatting);
      break;
    default:
      std::cout << "(unknown partition version)\n";
  }
}

void print_segment_v0(const vast::fbs::segment::v0* segment,
                      indentation& indent,
                      const formatting_options& formatting) {
  vast::uuid id;
  if (segment->uuid())
    unpack(*segment->uuid(), id);
  std::cout << indent << "Segment\n";
  indented_scope _(indent);
  std::cout << indent << "uuid: " << to_string(id) << "\n";
  std::cout << indent << "events: " << segment->events() << "\n";
  if (formatting.verbosity >= output_verbosity::verbose) {
    std::cout << indent << "table_slices:\n";
    indented_scope _(indent);
    size_t total_size = 0;
    for (auto flat_slice : *segment->slices()) {
      // We're intentionally creating a chunk without a deleter here, i.e., a
      // chunk that does not actually take ownership of its data. This is
      // necessary because we're accessing `vast::fbs::Segment` directly instead
      // of going through `vast::segment`, which has the necessary framing to
      // give out table slices that share the segment's lifetime.
      auto chunk = vast::chunk::make(flat_slice->data()->data(),
                                     flat_slice->data()->size(), {});
      auto slice
        = vast::table_slice(std::move(chunk), vast::table_slice::verify::no);
      std::cout << indent << slice.layout().name() << ": " << slice.rows()
                << " rows";
      if (formatting.print_bytesizes) {
        auto size = flat_slice->data()->size();
        std::cout << " (" << print_bytesize(size, formatting) << ")";
        total_size += size;
      }
      std::cout << '\n';
    }
    if (formatting.print_bytesizes)
      std::cout << indent << "total: " << print_bytesize(total_size, formatting)
                << "\n";
  }
}

void print_segment(const std::filesystem::path& path, indentation& indent,
                   const formatting_options& formatting) {
  auto segment = read_flatbuffer_file<vast::fbs::Segment>(path);
  if (!segment) {
    std::cout << "(error reading segment file " << path.string() << ")\n";
  }
  switch (segment->segment_type()) {
    case vast::fbs::segment::Segment::v0:
      print_segment_v0(segment->segment_as_v0(), indent, formatting);
      break;
    default:
      std::cout << "(unknown partition version)\n";
  }
}

int main(int argc, char** argv) {
  std::string raw_path;
  struct formatting_options format;
  format.print_bytesizes = true;
  format.verbosity = output_verbosity::verbose;
  for (int i = 1; i < argc; ++i) {
    auto arg = std::string_view{argv[i]};
    if (arg == "-h" || arg == "--human-readable") {
      format.print_bytesizes = true;
      format.human_readable_numbers = true;
    } else if (arg == "-s" || arg == "--print-bytesizes") {
      format.print_bytesizes = true;
    } else if (arg == "-v" || arg == "--verbose") {
      format.verbosity = output_verbosity::verbose;
    } else { // positional arg
      raw_path = arg;
    }
  }
  if (raw_path.empty()) {
    std::cerr << "Usage: ./lsvast <path/to/vast.db> [options]\n"
              << "Options:\n"
              << "  --verbose\n"
              << "  --print-bytesizes\n"
              << "  --human-readable\n";
    return 1;
  }
  if (raw_path.back() == '/')
    raw_path.resize(raw_path.size() - 1);
  const auto path = std::filesystem::path{raw_path};
  const auto kind = classify(path);
  if (!kind) {
    std::cerr << "Filesystem error with error code: " << kind.error().code()
              << std::endl;
    return 1;
  }
  if (kind == Kind::Unknown) {
    std::cerr << "Could not determine type of " << argv[1] << std::endl;
    return 1;
  }
  struct indentation indent;
  auto printer = printers.at(*kind);
  printer(path, indent, format);
  return 0;
}
