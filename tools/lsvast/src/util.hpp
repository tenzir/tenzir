//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2020 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <vast/io/read.hpp>
#include <vast/uuid.hpp>

#include <flatbuffers/flatbuffers.h>
#include <fmt/ostream.h> // Allow fmt to use the existing operator<<

#include <iomanip>
#include <ostream>
#include <sstream>
#include <vector>

#include "lsvast.hpp"

namespace lsvast {

struct indentation {
public:
  static constexpr const int TAB_WIDTH = 2;

  indentation() = default;

  void increase(int level = TAB_WIDTH) {
    levels_.push_back(level);
  }
  void decrease() {
    levels_.pop_back();
  }

  [[nodiscard]] const std::vector<int>& levels() const {
    return levels_;
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

inline std::ostream& operator<<(std::ostream& str, const indentation& indent) {
  for (auto level : indent.levels_)
    for (int i = 0; i < level; ++i)
      str << ' ';
  return str;
}

inline std::ostream&
operator<<(std::ostream& out, const vast::fbs::LegacyUUID* uuid) {
  if (!uuid || !uuid->data())
    return out << "(null)";
  auto old_flags = out.flags();
  for (size_t i = 0; i < uuid->data()->size(); ++i)
    out << std::hex << +uuid->data()->Get(i);
  out.flags(old_flags); // `std::hex` is sticky.
  return out;
}

template <size_t N>
std::ostream&
operator<<(std::ostream& str, const std::array<std::byte, N>& arr) {
  auto old_flags = str.flags();
  for (size_t i = 0; i < N; ++i) {
    str << std::hex << std::setw(2) << std::setfill('0')
        << std::to_integer<unsigned int>(arr[i]);
  }
  str.flags(old_flags); // `std::hex` is sticky.
  return str;
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

inline std::string
print_bytesize(size_t bytes, const formatting_options& formatting) {
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

} // namespace lsvast

template <>
struct fmt::formatter<lsvast::indentation> {
  template <class ParseContext>
  constexpr auto parse(ParseContext& ctx) {
    return ctx.begin();
  }

  template <class FormatContext>
  auto format(const lsvast::indentation& value, FormatContext& ctx) const {
    auto out = ctx.out();
    for (auto level : value.levels())
      for (int i = 0; i < level; ++i)
        out = format_to(out, " ");
    return out;
  }
};
