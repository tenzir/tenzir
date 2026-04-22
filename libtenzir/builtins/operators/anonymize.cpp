//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/ip.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/series_builder.hpp>
#include <tenzir/subnet.hpp>
#include <tenzir/table_slice.hpp>

#include <random>

namespace tenzir::plugins::anonymize {

namespace {

auto generate_value(builder_ref b, type const& ty, std::mt19937_64& rng)
  -> void;

auto generate_string(std::mt19937_64& rng) -> std::string {
  static constexpr auto chars = std::string_view{
    "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"};
  auto len_dist = std::uniform_int_distribution<size_t>{1, 32};
  auto char_dist = std::uniform_int_distribution<size_t>{0, chars.size() - 1};
  auto len = len_dist(rng);
  auto result = std::string{};
  result.reserve(len);
  for (auto i = size_t{0}; i < len; ++i) {
    result.push_back(chars[char_dist(rng)]);
  }
  return result;
}

auto generate_ip(std::mt19937_64& rng) -> ip {
  auto byte_dist = std::uniform_int_distribution<uint16_t>{0, 255};
  auto bytes = ip::byte_array{};
  // Generate a v4-mapped IPv6 address.
  std::copy(ip::v4_mapped_prefix.begin(), ip::v4_mapped_prefix.end(),
            bytes.begin());
  for (auto i = size_t{12}; i < 16; ++i) {
    bytes[i] = static_cast<uint8_t>(byte_dist(rng));
  }
  return ip{bytes};
}

auto generate_subnet(std::mt19937_64& rng) -> subnet {
  auto addr = generate_ip(rng);
  auto len_dist = std::uniform_int_distribution<uint8_t>{0, 32};
  return {addr, len_dist(rng)};
}

auto generate_record(record_ref rec, record_type const& rt,
                     std::mt19937_64& rng) -> void {
  for (auto field : rt.fields()) {
    generate_value(rec.field(field.name), field.type, rng);
  }
}

auto generate_value(builder_ref b, type const& ty, std::mt19937_64& rng)
  -> void {
  match(ty, [&]<class Ty>(Ty const& concrete_ty) {
    if constexpr (std::is_same_v<Ty, int64_type>) {
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
      b.data(generate_string(rng));
    } else if constexpr (std::is_same_v<Ty, ip_type>) {
      b.data(generate_ip(rng));
    } else if constexpr (std::is_same_v<Ty, subnet_type>) {
      b.data(generate_subnet(rng));
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
      auto fields = concrete_ty.fields();
      if (fields.empty()) {
        b.null();
      } else {
        auto dist = std::uniform_int_distribution<size_t>{0, fields.size() - 1};
        b.data(enumeration{fields[dist(rng)].key});
      }
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
    } else if constexpr (std::is_same_v<Ty, record_type>) {
      generate_record(b.record(), concrete_ty, rng);
    } else if constexpr (std::is_same_v<Ty, list_type>) {
      auto len_dist = std::uniform_int_distribution<int>{0, 5};
      auto len = len_dist(rng);
      auto list_builder = b.list();
      for (auto i = 0; i < len; ++i) {
        generate_value(list_builder, concrete_ty.value_type(), rng);
      }
    } else if constexpr (std::is_same_v<Ty, map_type>) {
      auto len_dist = std::uniform_int_distribution<int>{0, 5};
      auto len = len_dist(rng);
      auto list_builder = b.list();
      for (auto i = 0; i < len; ++i) {
        auto entry = list_builder.record();
        generate_value(entry.field("key"), concrete_ty.key_type(), rng);
        generate_value(entry.field("value"), concrete_ty.value_type(), rng);
      }
    } else {
      // null_type, secret_type, or any unknown type.
      b.null();
    }
  });
}

struct AnonymizeArgs {
  int64_t sample = 100;
  int64_t count = 100;
  std::optional<uint64_t> seed;
};

class Anonymize final : public Operator<table_slice, table_slice> {
public:
  explicit Anonymize(AnonymizeArgs args)
    : args_{args}, rng_{args.seed.value_or(std::random_device{}())} {
  }

  auto process(table_slice input, Push<table_slice>& push, OpCtx& ctx)
    -> Task<void> override {
    TENZIR_UNUSED(push, ctx);
    buffered_rows_ += input.rows();
    buffer_.push_back(std::move(input));
    co_return;
  }

  auto finalize(Push<table_slice>& push, OpCtx& ctx) -> Task<void> override {
    TENZIR_UNUSED(ctx);
    if (buffer_.empty()) {
      co_return;
    }
    // Count events per schema.
    auto schema_counts = std::map<type, int64_t>{};
    for (auto const& slice : buffer_) {
      schema_counts[slice.schema()]
        += detail::narrow_cast<int64_t>(slice.rows());
    }
    // Distribute count proportionally across schemas.
    auto total_sampled = int64_t{0};
    for (auto const& [_, n] : schema_counts) {
      total_sampled += n;
    }
    auto remaining = args_.count;
    auto it = schema_counts.begin();
    for (; it != schema_counts.end() and remaining > 0; ++it) {
      auto const& [schema, n] = *it;
      auto share = int64_t{};
      if (std::next(it) == schema_counts.end()) {
        // Last schema gets all remaining to avoid rounding issues.
        share = remaining;
      } else {
        share = static_cast<int64_t>(static_cast<double>(n)
                                     / static_cast<double>(total_sampled)
                                     * static_cast<double>(args_.count));
        share = std::min(share, remaining);
      }
      remaining -= share;
      // Generate events for this schema.
      auto const& rt = as<record_type>(schema);
      auto builder = series_builder{schema};
      for (auto i = int64_t{0}; i < share; ++i) {
        generate_record(builder.record(), rt, rng_);
      }
      for (auto& slice : builder.finish_as_table_slice(schema.name())) {
        co_await push(std::move(slice));
      }
    }
  }

  auto state() -> OperatorState override {
    if (buffered_rows_ >= args_.sample) {
      return OperatorState::done;
    }
    return OperatorState::unspecified;
  }

  auto snapshot(Serde& serde) -> void override {
    serde("buffer", buffer_);
    serde("buffered_rows", buffered_rows_);
  }

private:
  AnonymizeArgs args_;
  std::mt19937_64 rng_;
  std::vector<table_slice> buffer_;
  int64_t buffered_rows_ = 0;
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
    d.named("seed", &AnonymizeArgs::seed);
    return d.without_optimize();
  }
};

} // namespace

} // namespace tenzir::plugins::anonymize

TENZIR_REGISTER_PLUGIN(tenzir::plugins::anonymize::plugin)
