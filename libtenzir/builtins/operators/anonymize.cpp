//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/arrow_table_slice.hpp>
#include <tenzir/box.hpp>
#include <tenzir/detail/narrow.hpp>
#include <tenzir/detail/type_traits.hpp>
#include <tenzir/ip.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/secret.hpp>
#include <tenzir/series.hpp>
#include <tenzir/series_builder.hpp>
#include <tenzir/subnet.hpp>
#include <tenzir/table_slice.hpp>

#include <algorithm>
#include <cmath>
#include <cstddef>
#include <limits>
#include <map>
#include <optional>
#include <random>
#include <span>
#include <string>
#include <string_view>
#include <vector>

namespace tenzir::plugins::anonymize {

namespace {

struct NullStats {
  auto observe(bool is_null) -> void {
    ++total;
    if (is_null) {
      ++nulls;
    }
  }

  auto should_emit_null(std::mt19937_64& rng) const -> bool {
    if (total == 0 or nulls == total) {
      return true;
    }
    if (nulls == 0) {
      return false;
    }
    auto dist = std::uniform_int_distribution<uint64_t>{1, total};
    return dist(rng) <= nulls;
  }

  uint64_t total = 0;
  uint64_t nulls = 0;
};

template <class T>
class FrequencyStats {
public:
  auto observe(T value) -> void {
    for (auto& [existing, count] : counts_) {
      if (existing == value) {
        ++count;
        ++total_;
        return;
      }
    }
    counts_.emplace_back(value, uint64_t{1});
    ++total_;
  }

  auto empty() const -> bool {
    return total_ == 0;
  }

  auto draw(std::mt19937_64& rng) const -> T {
    TENZIR_ASSERT(total_ > 0);
    auto dist = std::uniform_int_distribution<uint64_t>{1, total_};
    auto pick = dist(rng);
    for (auto const& [value, count] : counts_) {
      if (pick <= count) {
        return value;
      }
      pick -= count;
    }
    TENZIR_UNREACHABLE();
  }

private:
  std::vector<std::pair<T, uint64_t>> counts_;
  uint64_t total_ = 0;
};

template <class T>
struct RangeStats {
  auto observe(T value) -> void {
    if (not min or value < *min) {
      min = value;
    }
    if (not max or value > *max) {
      max = value;
    }
  }

  auto empty() const -> bool {
    return not min;
  }

  std::optional<T> min;
  std::optional<T> max;
};

template <class T>
struct IntegralRangeStats {
  auto observe(T value) -> void {
    range.observe(value);
  }

  auto draw(builder_ref b, std::mt19937_64& rng, auto const&) -> void {
    if (range.empty()) {
      b.null();
      return;
    }
    auto dist = std::uniform_int_distribution<T>{*range.min, *range.max};
    b.data(dist(rng));
  }

  RangeStats<T> range;
};

struct DoubleRangeStats {
  auto observe(double value) -> void {
    if (std::isfinite(value)) {
      range.observe(value);
    }
  }

  auto draw(builder_ref b, std::mt19937_64& rng, auto const&) -> void {
    if (range.empty()) {
      b.null();
      return;
    }
    auto dist = std::uniform_real_distribution<double>{*range.min, *range.max};
    b.data(dist(rng));
  }

  RangeStats<double> range;
};

struct TimeRangeStats {
  auto observe(time value) -> void {
    range.observe(value.time_since_epoch().count());
  }

  auto draw(builder_ref b, std::mt19937_64& rng, auto const&) -> void {
    if (range.empty()) {
      b.null();
      return;
    }
    auto dist = std::uniform_int_distribution<int64_t>{*range.min, *range.max};
    b.data(time{} + duration{dist(rng)});
  }

  RangeStats<int64_t> range;
};

struct DurationRangeStats {
  auto observe(duration value) -> void {
    range.observe(value.count());
  }

  auto draw(builder_ref b, std::mt19937_64& rng, auto const&) -> void {
    if (range.empty()) {
      b.null();
      return;
    }
    auto dist = std::uniform_int_distribution<int64_t>{*range.min, *range.max};
    b.data(duration{dist(rng)});
  }

  RangeStats<int64_t> range;
};

template <class T>
struct FrequencyDrawStats {
  auto observe(T value) -> void {
    values.observe(value);
  }

  auto draw(builder_ref b, std::mt19937_64& rng, auto const&) -> void {
    if (values.empty()) {
      b.null();
      return;
    }
    b.data(values.draw(rng));
  }

  FrequencyStats<T> values;
};

struct NullOnlyStats {
  auto observe(auto) -> void {
  }

  auto draw(builder_ref b, std::mt19937_64&, auto const&) -> void {
    b.null();
  }
};

struct SecretStats {
  auto observe(auto) -> void {
  }

  auto draw(builder_ref b, std::mt19937_64&, auto const&) -> void {
    b.data(secret::make_literal("SECRET"));
  }
};

auto generate_random_ip(bool v4, std::mt19937_64& rng) -> ip;

struct ByteContentStats {
  auto observe_byte(uint8_t byte) -> void {
    bytes.observe(byte);
  }

  auto draw_byte(std::mt19937_64& rng) -> uint8_t {
    if (not bytes.empty()) {
      return bytes.draw(rng);
    }
    auto dist = std::uniform_int_distribution<uint16_t>{0, 255};
    return static_cast<uint8_t>(dist(rng));
  }

  FrequencyStats<uint8_t> bytes;
};

struct StringStats : ByteContentStats {
  auto observe(std::string_view value) -> void {
    lengths.observe(value.size());
    for (auto ch : value) {
      observe_byte(static_cast<uint8_t>(ch));
    }
  }

  auto draw(builder_ref b, std::mt19937_64& rng, auto const&) -> void {
    auto len = lengths.empty() ? size_t{0} : lengths.draw(rng);
    auto result = std::string{};
    result.reserve(len);
    for (auto i = size_t{0}; i < len; ++i) {
      result.push_back(static_cast<char>(draw_byte(rng)));
    }
    b.data(std::move(result));
  }

  FrequencyStats<size_t> lengths;
};

struct BlobStats : ByteContentStats {
  auto observe(blob_view value) -> void {
    lengths.observe(value.size());
    for (auto byte : value) {
      observe_byte(std::to_integer<uint8_t>(byte));
    }
  }

  auto draw(builder_ref b, std::mt19937_64& rng, auto const&) -> void {
    auto len = lengths.empty() ? size_t{0} : lengths.draw(rng);
    auto result = blob{};
    result.resize(len);
    for (auto& byte : result) {
      byte = static_cast<std::byte>(draw_byte(rng));
    }
    b.data(std::move(result));
  }

  FrequencyStats<size_t> lengths;
};

struct IpStats {
  auto observe(ip value) -> void {
    versions.observe(value.is_v4());
  }

  auto draw(builder_ref b, std::mt19937_64& rng, auto const&) -> void {
    auto v4 = versions.empty() ? true : versions.draw(rng);
    b.data(generate_random_ip(v4, rng));
  }

  FrequencyStats<bool> versions;
};

struct SubnetStats {
  auto observe(subnet value) -> void {
    versions.observe(value.network().is_v4());
    lengths.observe(value.length());
  }

  auto draw(builder_ref b, std::mt19937_64& rng, auto const&) -> void {
    auto v4 = versions.empty() ? true : versions.draw(rng);
    auto length = lengths.empty() ? uint8_t{0} : lengths.draw(rng);
    b.data(subnet{generate_random_ip(v4, rng), length});
  }

  FrequencyStats<bool> versions;
  FrequencyStats<uint8_t> lengths;
};

struct UnsupportedStats {
  auto observe(auto) -> void {
    TENZIR_UNREACHABLE();
  }

  auto draw(builder_ref, std::mt19937_64&, auto const&) -> void {
    TENZIR_UNREACHABLE();
  }
};

template <class Ty>
struct scalar_stats_trait {
  using data_type = type_to_data_t<Ty>;
  using stats_type = UnsupportedStats;
};

template <>
struct scalar_stats_trait<null_type> {
  using data_type = type_to_data_t<null_type>;
  using stats_type = NullOnlyStats;
};

template <>
struct scalar_stats_trait<bool_type> {
  using data_type = type_to_data_t<bool_type>;
  using stats_type = FrequencyDrawStats<data_type>;
};

template <>
struct scalar_stats_trait<int64_type> {
  using data_type = type_to_data_t<int64_type>;
  using stats_type = IntegralRangeStats<data_type>;
};

template <>
struct scalar_stats_trait<uint64_type> {
  using data_type = type_to_data_t<uint64_type>;
  using stats_type = IntegralRangeStats<data_type>;
};

template <>
struct scalar_stats_trait<double_type> {
  using data_type = type_to_data_t<double_type>;
  using stats_type = DoubleRangeStats;
};

template <>
struct scalar_stats_trait<duration_type> {
  using data_type = type_to_data_t<duration_type>;
  using stats_type = DurationRangeStats;
};

template <>
struct scalar_stats_trait<time_type> {
  using data_type = type_to_data_t<time_type>;
  using stats_type = TimeRangeStats;
};

template <>
struct scalar_stats_trait<string_type> {
  using data_type = type_to_data_t<string_type>;
  using stats_type = StringStats;
};

template <>
struct scalar_stats_trait<ip_type> {
  using data_type = type_to_data_t<ip_type>;
  using stats_type = IpStats;
};

template <>
struct scalar_stats_trait<subnet_type> {
  using data_type = type_to_data_t<subnet_type>;
  using stats_type = SubnetStats;
};

template <>
struct scalar_stats_trait<enumeration_type> {
  using data_type = type_to_data_t<enumeration_type>;
  using stats_type = FrequencyDrawStats<data_type>;
};

template <>
struct scalar_stats_trait<blob_type> {
  using data_type = type_to_data_t<blob_type>;
  using stats_type = BlobStats;
};

template <>
struct scalar_stats_trait<map_type> {
  using data_type = type_to_data_t<map_type>;
  using stats_type = UnsupportedStats;
};

template <>
struct scalar_stats_trait<secret_type> {
  using data_type = type_to_data_t<secret_type>;
  using stats_type = SecretStats;
};

auto make_validity_bitmap(int64_t count, NullStats const& nulls,
                          std::mt19937_64& rng)
  -> std::shared_ptr<arrow::Buffer> {
  auto builder = arrow::TypedBufferBuilder<bool>{arrow_memory_pool()};
  check(builder.Reserve(count));
  for (auto i = int64_t{0}; i < count; ++i) {
    builder.UnsafeAppend(not nulls.should_emit_null(rng));
  }
  return check(builder.Finish());
}

auto generate_random_ip(bool v4, std::mt19937_64& rng) -> ip {
  auto byte_dist = std::uniform_int_distribution<uint16_t>{0, 255};
  auto bytes = ip::byte_array{};
  if (v4) {
    std::copy(ip::v4_mapped_prefix.begin(), ip::v4_mapped_prefix.end(),
              bytes.begin());
    for (auto i = size_t{12}; i < 16; ++i) {
      bytes[i] = static_cast<uint8_t>(byte_dist(rng));
    }
  } else {
    for (auto& byte : bytes) {
      byte = static_cast<uint8_t>(byte_dist(rng));
    }
  }
  return ip{bytes};
}

class SeriesGenerator {
public:
  SeriesGenerator() = default;
  SeriesGenerator(SeriesGenerator&&) noexcept = default;
  SeriesGenerator& operator=(SeriesGenerator&&) noexcept = default;
  SeriesGenerator(const SeriesGenerator&) = delete;
  auto operator=(const SeriesGenerator&) -> SeriesGenerator& = delete;
  virtual ~SeriesGenerator() = default;

  virtual auto generate(int64_t count, std::mt19937_64& rng) -> series = 0;
};

struct FieldGenerator {
  std::string name;
  Box<SeriesGenerator> generator;
};

class RecordGenerator final : public SeriesGenerator {
public:
  explicit RecordGenerator(type schema, std::vector<FieldGenerator> fields,
                           bool fully_random, bool preserve_nulls,
                           NullStats nulls)
    : schema_{std::move(schema)},
      fields_{std::move(fields)},
      fully_random_{fully_random},
      preserve_nulls_{preserve_nulls},
      nulls_{nulls} {
  }

  auto generate(int64_t count, std::mt19937_64& rng) -> series override {
    return generate_record_series(count, rng);
  }

  auto generate_table_slice(uint64_t count, std::mt19937_64& rng)
    -> table_slice {
    auto const rows = detail::narrow<int64_t>(count);
    auto record = generate_record_series(rows, rng);
    auto batch = record_batch_from_struct_array(schema_.to_arrow_schema(),
                                                *record.array);
    return table_slice{batch, schema_};
  }

private:
  auto generate_record_series(int64_t count, std::mt19937_64& rng)
    -> basic_series<record_type> {
    auto fields = std::vector<series_field>{};
    fields.reserve(fields_.size());
    for (auto& field : fields_) {
      fields.push_back({
        .name = field.name,
        .data = field.generator->generate(count, rng),
      });
    }
    auto null_bitmap = std::shared_ptr<arrow::Buffer>{};
    if (not fully_random_ and preserve_nulls_) {
      null_bitmap = make_validity_bitmap(count, nulls_, rng);
    }
    auto origin = make_struct_array(count, std::move(null_bitmap),
                                    arrow::FieldVector{}, arrow::ArrayVector{});
    return make_record_series(fields, *origin);
  }

  type schema_;
  std::vector<FieldGenerator> fields_;
  bool fully_random_;
  bool preserve_nulls_;
  NullStats nulls_;
};

class ListGenerator final : public SeriesGenerator {
public:
  explicit ListGenerator(list_type type, Box<SeriesGenerator> value_generator,
                         bool fully_random, NullStats nulls,
                         FrequencyStats<int64_t> lengths)
    : type_{std::move(type)},
      value_generator_{std::move(value_generator)},
      fully_random_{fully_random},
      nulls_{nulls},
      lengths_{std::move(lengths)} {
  }

  auto generate(int64_t count, std::mt19937_64& rng) -> series override {
    auto builder = series_builder{type{type_}};
    auto len_dist = std::uniform_int_distribution<int>{0, 5};
    for (auto i = int64_t{0}; i < count; ++i) {
      if (not fully_random_ and nulls_.should_emit_null(rng)) {
        builder.null();
        continue;
      }
      auto list_builder = builder.list();
      auto len = int64_t{};
      if (fully_random_) {
        len = len_dist(rng);
      } else if (not lengths_.empty()) {
        len = lengths_.draw(rng);
      }
      for (auto j = int64_t{0}; j < len; ++j) {
        auto value = value_generator_->generate(1, rng);
        auto values = value.values3();
        auto it = values.begin();
        TENZIR_ASSERT(it != values.end());
        list_builder.data(*it);
      }
    }
    auto result = builder.finish_assert_one_array();
    auto list = result.as<list_type>();
    TENZIR_ASSERT(list);
    return std::move(*list);
  }

private:
  list_type type_;
  Box<SeriesGenerator> value_generator_;
  bool fully_random_;
  NullStats nulls_;
  FrequencyStats<int64_t> lengths_;
};

template <class Ty>
class ScalarGenerator final : public SeriesGenerator {
public:
  explicit ScalarGenerator(type type, Ty concrete_type, bool fully_random,
                           std::span<series const> samples)
    : type_{std::move(type)},
      concrete_type_{std::move(concrete_type)},
      fully_random_{fully_random} {
    if (not fully_random_) {
      collect(samples);
    }
  }

  auto generate(int64_t count, std::mt19937_64& rng) -> series override {
    auto build = [&](auto append) -> series {
      auto builder = series_builder{type_};
      for (auto i = int64_t{0}; i < count; ++i) {
        append(builder);
      }
      return builder.finish_assert_one_array();
    };
    return build([&](builder_ref b) {
      if (fully_random_) {
        append_random(b, rng);
      } else {
        append_from_stats(b, rng);
      }
    });
  }

private:
  auto collect(std::span<series const> samples) -> void {
    for (auto const& sample : samples) {
      auto typed = sample.as<Ty>();
      TENZIR_ASSERT(typed);
      for (auto value : typed->values3()) {
        nulls_.observe(not value);
        if (value) {
          stats_.observe(*value);
        }
      }
    }
  }

  auto append_from_stats(builder_ref b, std::mt19937_64& rng) -> void {
    if (nulls_.should_emit_null(rng)) {
      b.null();
      return;
    }
    stats_.draw(b, rng, concrete_type_);
  }

  auto append_random(builder_ref b, std::mt19937_64& rng) -> void {
    if constexpr (std::is_same_v<Ty, null_type>) {
      b.null();
    } else if constexpr (std::is_same_v<Ty, int64_type>) {
      auto dist = std::uniform_int_distribution<int64_t>{
        std::numeric_limits<int64_t>::min(),
        std::numeric_limits<int64_t>::max()};
      b.data(dist(rng));
    } else if constexpr (std::is_same_v<Ty, uint64_type>) {
      auto dist = std::uniform_int_distribution<uint64_t>{
        0, std::numeric_limits<uint64_t>::max()};
      b.data(dist(rng));
    } else if constexpr (std::is_same_v<Ty, double_type>) {
      auto dist = std::uniform_real_distribution<double>{0.0, 1.0};
      b.data(dist(rng));
    } else if constexpr (std::is_same_v<Ty, bool_type>) {
      auto dist = std::uniform_int_distribution<int>{0, 1};
      b.data(dist(rng) != 0);
    } else if constexpr (std::is_same_v<Ty, string_type>) {
      static constexpr auto chars = std::string_view{
        "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"};
      auto len_dist = std::uniform_int_distribution<size_t>{1, 32};
      auto char_dist
        = std::uniform_int_distribution<size_t>{0, chars.size() - 1};
      auto len = len_dist(rng);
      auto result = std::string{};
      result.reserve(len);
      for (auto i = size_t{0}; i < len; ++i) {
        result.push_back(chars[char_dist(rng)]);
      }
      b.data(std::move(result));
    } else if constexpr (std::is_same_v<Ty, ip_type>) {
      b.data(generate_random_ip(true, rng));
    } else if constexpr (std::is_same_v<Ty, subnet_type>) {
      auto len_dist = std::uniform_int_distribution<uint8_t>{0, 32};
      b.data(subnet{generate_random_ip(true, rng), len_dist(rng)});
    } else if constexpr (std::is_same_v<Ty, time_type>) {
      // Random timestamp within a ~100 year range from epoch.
      auto dist = std::uniform_int_distribution<int64_t>{
        0, 100LL * 365 * 24 * 3600 * 1'000'000'000LL};
      b.data(time{} + duration{dist(rng)});
    } else if constexpr (std::is_same_v<Ty, duration_type>) {
      auto dist = std::uniform_int_distribution<int64_t>{
        0, 365LL * 24 * 3600 * 1'000'000'000LL};
      b.data(duration{dist(rng)});
    } else if constexpr (std::is_same_v<Ty, enumeration_type>) {
      auto fields = concrete_type_.fields();
      if (fields.empty()) {
        b.null();
        return;
      }
      auto dist = std::uniform_int_distribution<size_t>{0, fields.size() - 1};
      b.data(detail::narrow_cast<enumeration>(fields[dist(rng)].key));
    } else if constexpr (std::is_same_v<Ty, blob_type>) {
      auto len_dist = std::uniform_int_distribution<size_t>{1, 32};
      auto byte_dist = std::uniform_int_distribution<uint16_t>{0, 255};
      auto len = len_dist(rng);
      auto result = blob{};
      result.resize(len);
      for (auto& byte : result) {
        byte = static_cast<std::byte>(byte_dist(rng));
      }
      b.data(std::move(result));
    } else if constexpr (std::is_same_v<Ty, map_type>) {
      TENZIR_UNREACHABLE();
    } else if constexpr (std::is_same_v<Ty, secret_type>) {
      b.data(secret::make_literal("SECRET"));
    } else {
      static_assert(detail::always_false_v<Ty>, "unhandled type");
    }
  }

  type type_;
  Ty concrete_type_;
  bool fully_random_;
  NullStats nulls_;
  typename scalar_stats_trait<Ty>::stats_type stats_;
};

auto make_generator(std::span<series const> samples, bool fully_random)
  -> Box<SeriesGenerator>;

auto collect_record_field_samples(std::span<series const> samples,
                                  std::string_view name)
  -> std::vector<series> {
  auto result = std::vector<series>{};
  for (auto const& sample : samples) {
    auto record = sample.as<record_type>();
    if (not record) {
      continue;
    }
    if (auto field = record->field(name)) {
      result.push_back(std::move(*field));
    }
  }
  return result;
}

auto collect_list_value_samples(list_type const& ty,
                                std::span<series const> samples)
  -> std::vector<series> {
  auto result = std::vector<series>{};
  for (auto const& sample : samples) {
    auto list = sample.as<list_type>();
    if (not list) {
      continue;
    }
    result.emplace_back(ty.value_type(), list->array->values());
  }
  return result;
}

auto collect_record_nulls(std::span<series const> samples) -> NullStats {
  auto result = NullStats{};
  for (auto const& sample : samples) {
    auto record = sample.as<record_type>();
    TENZIR_ASSERT(record);
    for (auto value : record->values3()) {
      result.observe(not value);
    }
  }
  return result;
}

auto collect_list_nulls_and_lengths(std::span<series const> samples)
  -> std::pair<NullStats, FrequencyStats<int64_t>> {
  auto nulls = NullStats{};
  auto lengths = FrequencyStats<int64_t>{};
  for (auto const& sample : samples) {
    auto list = sample.as<list_type>();
    TENZIR_ASSERT(list);
    for (auto i = int64_t{0}; i < list->array->length(); ++i) {
      auto is_null = list->array->IsNull(i);
      nulls.observe(is_null);
      if (not is_null) {
        lengths.observe(list->array->value_length(i));
      }
    }
  }
  return {nulls, std::move(lengths)};
}

auto make_record_generator(std::span<series const> samples, bool fully_random,
                           bool preserve_nulls) -> RecordGenerator {
  TENZIR_ASSERT(not samples.empty());
  auto const& ty = samples.front().type;
  auto const& record_ty = as<record_type>(ty);
  auto fields = std::vector<FieldGenerator>{};
  fields.reserve(record_ty.num_fields());
  for (auto const& field : record_ty.fields()) {
    auto field_samples = collect_record_field_samples(samples, field.name);
    fields.push_back({
      .name = std::string{field.name},
      .generator = make_generator(field_samples, fully_random),
    });
  }
  return RecordGenerator{ty, std::move(fields), fully_random, preserve_nulls,
                         collect_record_nulls(samples)};
}

auto make_generator(std::span<series const> samples, bool fully_random)
  -> Box<SeriesGenerator> {
  TENZIR_ASSERT(not samples.empty());
  auto const& ty = samples.front().type;
  return match(ty, [&]<class Ty>(Ty const& concrete_ty) -> Box<SeriesGenerator> {
    if constexpr (std::is_same_v<Ty, record_type>) {
      return Box<SeriesGenerator>{
        make_record_generator(samples, fully_random, true)};
    } else if constexpr (std::is_same_v<Ty, list_type>) {
      auto value_samples = collect_list_value_samples(concrete_ty, samples);
      auto [nulls, lengths] = collect_list_nulls_and_lengths(samples);
      return Box<SeriesGenerator>{
        ListGenerator{concrete_ty, make_generator(value_samples, fully_random),
                      fully_random, nulls, std::move(lengths)}};
    } else {
      return Box<SeriesGenerator>{
        ScalarGenerator<Ty>{ty, concrete_ty, fully_random, samples}};
    }
  });
}

auto make_top_level_generator(std::span<table_slice const> slices,
                              bool fully_random) -> RecordGenerator {
  TENZIR_ASSERT(not slices.empty());
  auto samples = std::vector<series>{};
  samples.reserve(slices.size());
  for (auto const& slice : slices) {
    samples.emplace_back(slice);
  }
  return make_record_generator(samples, fully_random, false);
}

struct AnonymizeArgs {
  uint64_t sample = 100;
  uint64_t count = 100;
  bool fully_random = false;
  Option<uint64_t> seed;
};

class Anonymize final : public Operator<table_slice, table_slice> {
public:
  explicit Anonymize(AnonymizeArgs args)
    : args_{args}, rng_{args.seed.unwrap_or(std::random_device{}())} {
  }

  auto process(table_slice input, Push<table_slice>& push, OpCtx& ctx)
    -> Task<void> override {
    TENZIR_UNUSED(push, ctx);
    total_rows_ += input.rows();
    auto& bucket = data_[input.schema()];
    bucket.rows += input.rows();
    bucket.slices.push_back(std::move(input));
    co_return;
  }

  auto finalize(Push<table_slice>& push, OpCtx& ctx)
    -> Task<FinalizeBehavior> override {
    TENZIR_UNUSED(ctx);
    if (data_.empty()) {
      co_return FinalizeBehavior::done;
    }
    auto remaining = args_.count;
    for (auto it = data_.begin(); it != data_.end() and remaining > 0; ++it) {
      auto const& bucket = it->second;
      auto share = uint64_t{};
      if (std::next(it) == data_.end()) {
        // Last schema gets all remaining to avoid rounding issues.
        share = remaining;
      } else {
        share = static_cast<int64_t>(static_cast<double>(bucket.rows)
                                     / static_cast<double>(total_rows_)
                                     * static_cast<double>(args_.count));
        share = std::min(share, remaining);
      }
      remaining -= share;
      // Generate events for this schema.
      if (share > 0) {
        auto slices = std::span<table_slice const>{bucket.slices.data(),
                                                   bucket.slices.size()};
        auto generator = make_top_level_generator(slices, args_.fully_random);
        co_await push(generator.generate_table_slice(share, rng_));
      }
    }
    co_return FinalizeBehavior::done;
  }

  auto state() -> OperatorState override {
    if (total_rows_ >= args_.sample) {
      return OperatorState::done;
    }
    return OperatorState::normal;
  }

  auto snapshot(Serde& serde) -> void override {
    TENZIR_UNUSED(serde);
    diagnostic::error("`anonymize cannot be snapshotted").throw_();
  }

private:
  struct Bucket {
    std::vector<table_slice> slices;
    std::size_t rows;
  };

  AnonymizeArgs args_;
  std::mt19937_64 rng_;
  /// We want an ordered map here for seeding to make sense.
  std::map<type, Bucket> data_;
  uint64_t total_rows_ = 0;
};

class plugin final : public virtual OperatorPlugin {
public:
  auto name() const -> std::string override {
    return "anonymize";
  }

  auto describe() const -> Description override {
    auto d = Describer<AnonymizeArgs, Anonymize>{};
    d.named_optional("sample", &AnonymizeArgs::sample);
    d.named_optional("count", &AnonymizeArgs::count);
    d.named_optional("fully_random", &AnonymizeArgs::fully_random);
    d.named("seed", &AnonymizeArgs::seed);
    return d.without_optimize();
  }
};

} // namespace

} // namespace tenzir::plugins::anonymize

TENZIR_REGISTER_PLUGIN(tenzir::plugins::anonymize::plugin)
