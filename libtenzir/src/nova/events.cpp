//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/nova/events.hpp"

#include "tenzir/nova/array_builder.hpp"
#include "tenzir/nova/bitz.hpp"
#include "tenzir/try.hpp"

#include <algorithm>
#include <array>
#include <cstdint>
#include <limits>
#include <string>
#include <string_view>

namespace tenzir::nova {
namespace {

// A multi-payload actor message uses the same framing as the file format.
// Unframed payloads start with their byte order (0 or 1), never this magic.
constexpr auto frame_magic
  = std::array{std::byte{'T'}, std::byte{'N'}, std::byte{'Z'}, std::byte{'2'}};

auto append_frame(std::vector<std::byte>& output,
                  std::span<std::byte const> payload) -> void {
  output.insert(output.end(), frame_magic.begin(), frame_magic.end());
  auto const length = static_cast<std::uint64_t>(payload.size());
  for (auto i = 0; i < 8; ++i) {
    output.push_back(static_cast<std::byte>((length >> (8 * i)) & 0xff));
  }
  output.insert(output.end(), payload.begin(), payload.end());
}

auto is_framed(std::span<std::byte const> payload) -> bool {
  return payload.size() >= frame_magic.size()
         and std::ranges::equal(payload.first(frame_magic.size()), frame_magic);
}

} // namespace

Events::Events(Array<Record> data, storage::BitMap mask, Meta meta)
  : data{std::move(data)}, mask{std::move(mask)}, meta{std::move(meta)} {
}

auto encode_events(Events const& events)
  -> Result<std::vector<std::byte>, std::string> {
  if (events.length() == 0) {
    return std::vector<std::byte>{};
  }
  auto options = bitz::EncodeOptions{};
  options.max_rows = std::numeric_limits<storage::Index>::max();
  options.max_array_length = options.max_rows;
  // Match decode_events: actor transport does not use file-reader size limits.
  options.max_frame_bytes = std::numeric_limits<std::uint64_t>::max();
  options.max_logical_slots = std::numeric_limits<std::uint64_t>::max();
  options.max_decoded_bytes = std::numeric_limits<std::uint64_t>::max();
  auto first = Option<std::vector<std::byte>>{};
  auto output = std::vector<std::byte>{};
  for (auto&& result : bitz::encode_batches(
         bitz::Batch{events.data, events.mask, events.meta}, options)) {
    TRY(auto payload, std::move(result));
    if (not first) {
      first = std::move(payload);
      continue;
    }
    if (output.empty()) {
      append_frame(output, *first);
      *first = std::vector<std::byte>{};
    }
    append_frame(output, payload);
  }
  // Preserve the existing wire representation and avoid rebuilding event rows
  // whenever the batch fits a single payload.
  TENZIR_ASSERT(first);
  return output.empty() ? std::move(*first) : std::move(output);
}

auto decode_events(std::span<std::byte const> payload)
  -> Result<Events, std::string> {
  if (payload.empty()) {
    return Events{};
  }
  // CAF has already received this trusted actor payload in full. Keep Bitz's
  // structural limits, but not its external file-reader size limits.
  auto limits = bitz::default_decode_limits;
  limits.max_frame_bytes = payload.size();
  limits.max_rows = std::numeric_limits<storage::Index>::max();
  limits.max_array_length = limits.max_rows;
  limits.max_logical_slots = std::numeric_limits<std::uint64_t>::max();
  limits.max_decoded_bytes = std::numeric_limits<std::uint64_t>::max();
  auto decode_one
    = [&](std::span<std::byte const> bytes) -> Result<Events, std::string> {
    TRY(auto batch, bitz::decode(bytes, limits));
    auto records = std::move(batch.data).try_as<Record>();
    if (not records) {
      return Err{"event batch root must be a record array"};
    }
    return Events{std::move(*records), std::move(batch.mask),
                  std::move(batch.meta)};
  };
  if (not is_framed(payload)) {
    return decode_one(payload);
  }
  auto data = ArrayBuilder<Record>{};
  auto mask = storage::BitMap::Builder{};
  auto names = ArrayBuilder<String>{};
  auto times = ArrayBuilder<Time>{};
  auto internal = ArrayBuilder<Bool>{};
  auto rows = storage::Index{0};
  while (not payload.empty()) {
    if (payload.size() < frame_magic.size() + 8 or not is_framed(payload)) {
      return Err{"invalid or truncated Bitz event frame header"};
    }
    payload = payload.subspan(frame_magic.size());
    auto length = std::uint64_t{0};
    for (auto i = 0; i < 8; ++i) {
      length
        |= std::to_integer<std::uint64_t>(payload[static_cast<std::size_t>(i)])
           << (8 * i);
    }
    payload = payload.subspan(8);
    if (length > payload.size()) {
      return Err{"truncated Bitz event frame"};
    }
    TRY(auto events,
        decode_one(payload.first(static_cast<std::size_t>(length))));
    payload = payload.subspan(static_cast<std::size_t>(length));
    if (events.length() > std::numeric_limits<storage::Index>::max() - rows) {
      return Err{"Bitz event frames exceed the supported row count"};
    }
    rows += events.length();
    for (auto i = storage::Index{0}; i < events.length(); ++i) {
      auto record = data.record();
      if (events.mask.get(i)) {
        for (auto [name, value] : events.data.get(i)) {
          append_row(record.field(name), value);
        }
      }
      mask.emplace_back(events.mask.get(i));
      names.data(*events.meta.name.get(i));
      times.data(*events.meta.import_time.get(i));
      internal.data(*events.meta.internal.get(i));
    }
  }
  return Events{data.finish(), mask.finish(),
                Events::Meta{names.finish(), times.finish(),
                             internal.finish()}};
}

auto Events::Meta::make_empty(storage::Index length, std::string_view name)
  -> Meta {
  using ConstantString
    = storage::ConstantStorage<std::string, std::string_view>;
  return Meta{
    .name = Array<String>{ConstantString{length, std::string{name}}},
    .import_time = Array<Time>{storage::ConstantStorage<Time>{length, {}}},
    .internal = Array<Bool>{storage::BitMap{length, false}},
  };
}

auto subslice(Events const& events, storage::Index begin, storage::Index end)
  -> Events {
  TENZIR_ASSERT_LEQ(0, begin);
  TENZIR_ASSERT_LEQ(begin, end);
  TENZIR_ASSERT_LEQ(end, events.length());
  auto data = ArrayBuilder<Record>{};
  auto mask = storage::BitMap::Builder{};
  auto names = ArrayBuilder<String>{};
  auto import_times = ArrayBuilder<Time>{};
  auto internal = ArrayBuilder<Bool>{};
  for (auto i = begin; i < end; ++i) {
    auto record = data.record();
    for (auto [name, value] : events.data.get(i)) {
      append_row(record.field(name), value);
    }
    mask.emplace_back(events.mask.get(i));
    names.data(*events.meta.name.get(i));
    import_times.data(*events.meta.import_time.get(i));
    internal.data(*events.meta.internal.get(i));
  }
  return Events{data.finish(), mask.finish(),
                Events::Meta{names.finish(), import_times.finish(),
                             internal.finish()}};
}

} // namespace tenzir::nova
