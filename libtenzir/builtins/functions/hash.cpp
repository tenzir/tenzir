//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2021 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/argument_parser2.hpp>
#include <tenzir/arrow_table_slice.hpp>
#include <tenzir/arrow_utils.hpp>
#include <tenzir/as_bytes.hpp>
#include <tenzir/cast.hpp>
#include <tenzir/concept/convertible/to.hpp>
#include <tenzir/concept/parseable/core.hpp>
#include <tenzir/concept/parseable/tenzir/option_set.hpp>
#include <tenzir/concept/parseable/tenzir/pipeline.hpp>
#include <tenzir/detail/base64.hpp>
#include <tenzir/detail/coding.hpp>
#include <tenzir/detail/inspection_common.hpp>
#include <tenzir/detail/narrow.hpp>
#include <tenzir/detail/type_traits.hpp>
#include <tenzir/error.hpp>
#include <tenzir/hash/concepts.hpp>
#include <tenzir/hash/hash_append.hpp>
#include <tenzir/hash/md5.hpp>
#include <tenzir/hash/sha.hpp>
#include <tenzir/hash/xxhash.hpp>
#include <tenzir/location.hpp>
#include <tenzir/nova/array_builder.hpp>
#include <tenzir/nova/bitmap_iteration.hpp>
#include <tenzir/nova/eval.hpp>
#include <tenzir/nova/function_plugin.hpp>
#include <tenzir/nova/type_system.hpp>
#include <tenzir/optional.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/secret.hpp>
#include <tenzir/tql2/eval.hpp>
#include <tenzir/tql2/plugin.hpp>

#include <arrow/array/array_binary.h>
#include <arrow/scalar.h>
#include <fmt/format.h>

#include <string_view>

namespace tenzir::plugins::hash {

namespace {

/// Feeds one Nova row into `h`. Strings and blobs contribute only their bytes,
/// while other values are hashed directly through their Nova row views.
/// Secrets are hashed as `***` and set `saw_secret`.
template <class HashAlgorithm>
auto hash_row(HashAlgorithm& h, nova::RowView<nova::Data> const& row,
              bool& saw_secret) -> void {
  match(row, [&]<typename V>(nova::RowView<V> value) {
    if constexpr (std::same_as<V, nova::String>) {
      h.add(as_bytes(*value));
    } else if constexpr (std::same_as<V, nova::Blob>) {
      h.add(*value);
    } else if constexpr (std::same_as<V, nova::Secret>) {
      saw_secret = true;
      hash_append(h, std::string_view{"***"});
    } else if constexpr (std::same_as<V, nova::List>) {
      for (auto element : value) {
        hash_row(h, element, saw_secret);
      }
      hash_append(h, value.length());
    } else if constexpr (std::same_as<V, nova::Record>) {
      auto size = size_t{0};
      for (auto [name, field] : value) {
        hash_append(h, name);
        hash_row(h, field, saw_secret);
        ++size;
      }
      hash_append(h, size);
    } else {
      hash_append(h, *value);
    }
  });
}

template <class HashAlgorithm>
auto hex_digest(HashAlgorithm& hasher) -> std::string {
  auto digest = hasher.finish();
  if constexpr (concepts::integer<typename HashAlgorithm::result_type>
                and HashAlgorithm::endian == std::endian::little) {
    digest = detail::to_network_order(digest);
  }
  return detail::hexify(as_bytes(digest));
}

auto warn_secret(nova::EvalFrame const& frame, location source) -> void {
  diagnostic::warning("secret values are hashed as `***`")
    .primary(source)
    .emit(frame);
}

struct HashArgs {
  nova::ValueArgument x;
  Option<std::string> seed;
};

template <reusable_hash HashAlgorithm>
class HashFunction {
public:
  static auto eval(HashArgs const& args, nova::EvalFrame frame)
    -> nova::Array<nova::Data> {
    auto hasher = HashAlgorithm{};
    auto builder = nova::ArrayBuilder<nova::Data>{};
    auto saw_secret = false;
    for (auto index : nova::storage::true_bits(frame.mask())) {
      builder.skip_n(index - builder.length());
      hasher.reset();
      if (args.seed) {
        hasher.add(as_bytes(*args.seed));
      }
      hash_row(hasher, args.x.data.get(index), saw_secret);
      builder.data(std::string_view{hex_digest(hasher)});
    }
    builder.skip_n(frame.length() - builder.length());
    if (saw_secret) {
      warn_secret(frame, args.x.source);
    }
    return builder.finish();
  }
};

template <reusable_hash HashAlgorithm, detail::string_literal Name>
class fun : public virtual nova::FunctionPlugin {
  auto name() const -> std::string override {
    return fmt::format("hash_{}", Name);
  }

  auto is_deterministic() const -> bool override {
    return true;
  }

  auto describe() const -> nova::FunctionDescription override {
    auto d = nova::FunctionDescriber<HashArgs, HashFunction<HashAlgorithm>>{};
    d.positional("x", &HashArgs::x, "any");
    d.named("seed", &HashArgs::seed);
    return std::move(d).finish();
  }

  auto make_function(function_invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
    auto expr = ast::expression{};
    auto seed = Option<std::string>{};
    TRY(argument_parser2::function(name())
          .positional("x", expr, "any")
          .named("seed", seed)
          .parse(inv, ctx));
    return function_use::make(
      [expr_ = std::move(expr), seed_ = std::move(seed)](evaluator eval,
                                                         session) -> series {
        const auto& s = eval(expr_);
        HashAlgorithm hasher{};
        auto hash = [&](const auto& x) {
          // For strings and blobs, users expect the same digest as in other
          // tools, i.e., hashing only the byte sequence. This includes hashing
          // the seed bytes. Other types continue to use `hash_append(...)`.
          hasher.reset();
          if (seed_) {
            hasher.add(as_bytes(*seed_));
          }
          auto f = detail::overload{
            [&](const auto& value) {
              hash_append(hasher, value);
            },
            [&](std::string_view str) {
              hasher.add(as_bytes(str));
            },
            [&](blob_view blob) {
              hasher.add(blob);
            },
          };
          match(x, f);
          return hasher.finish();
        };
        auto b = string_type::make_arrow_builder(arrow_memory_pool());
        for (const auto& value : s.values()) {
          auto digest = hash(value);
          if constexpr (concepts::integer<typename HashAlgorithm::result_type>
                        and HashAlgorithm::endian == std::endian::little) {
            digest = detail::to_network_order(digest);
          }
          auto hex = detail::hexify(as_bytes(digest));
          check(b->Append(hex));
        }
        return {string_type{}, finish(*b)};
      });
  }
};

TENZIR_ENUM(hmac_algorithm, sha256, sha512, sha384, sha1, md5);

template <class Hmac>
auto hmac_digest(data_view3 data, std::span<std::byte const> key)
  -> std::string {
  auto mac = Hmac{key};
  match(
    data,
    [&](std::string_view str) {
      mac.add(as_bytes(str));
    },
    [&](blob_view blob) {
      mac.add(blob);
    },
    [&](auto const& value) {
      hash_append(mac, value);
    });
  return detail::hexify(as_bytes(mac.finish()));
}

template <class Hmac>
auto hmac_digest(nova::RowView<nova::Data> const& data,
                 std::span<std::byte const> key, bool& saw_secret)
  -> std::string {
  auto mac = Hmac{key};
  hash_row(mac, data, saw_secret);
  return detail::hexify(as_bytes(mac.finish()));
}

template <class Data, class... Extra>
auto compute_hmac(Data const& data, std::span<std::byte const> key,
                  hmac_algorithm algorithm, Extra&... extra) -> std::string {
  switch (algorithm) {
    case hmac_algorithm::sha256:
      return hmac_digest<tenzir::hmac_sha256>(data, key, extra...);
    case hmac_algorithm::sha512:
      return hmac_digest<tenzir::hmac_sha512>(data, key, extra...);
    case hmac_algorithm::sha384:
      return hmac_digest<tenzir::hmac_sha384>(data, key, extra...);
    case hmac_algorithm::sha1:
      return hmac_digest<tenzir::hmac_sha1>(data, key, extra...);
    case hmac_algorithm::md5:
      return hmac_digest<tenzir::hmac_md5>(data, key, extra...);
  }
  TENZIR_UNREACHABLE();
}

struct HmacArgs {
  nova::ValueArgument data;
  located<nova::Secret> key;
  located<std::string> algorithm{"sha256", location::unknown};
  hmac_algorithm parsed_algorithm = hmac_algorithm::sha256;
};

class HmacFunction {
public:
  static auto eval(HmacArgs const& args, nova::EvalFrame frame)
    -> nova::Array<nova::Data> {
    auto builder = nova::ArrayBuilder<nova::Data>{};
    auto saw_secret = false;
    for (auto index : nova::storage::bitmap_iteration(frame.mask())) {
      if (not index) {
        builder.skip();
        continue;
      }
      auto row = args.data.data.get(*index);
      if (is<nova::RowView<nova::Null>>(row)) {
        builder.null();
        continue;
      }
      builder.data(std::string_view{compute_hmac(
        row, args.key.inner.data, args.parsed_algorithm, saw_secret)});
    }
    if (saw_secret) {
      warn_secret(frame, args.data.source);
    }
    return builder.finish();
  }
};

class hmac final : public virtual nova::FunctionPlugin {
public:
  auto name() const -> std::string override {
    return "hmac";
  }

  auto is_deterministic() const -> bool override {
    return true;
  }

  auto describe() const -> nova::FunctionDescription override {
    auto d = nova::FunctionDescriber<HmacArgs, HmacFunction>{};
    d.positional("data", &HmacArgs::data, "any");
    d.positional("key", &HmacArgs::key);
    d.named_optional("algorithm", &HmacArgs::algorithm);
    d.validate([](HmacArgs& args, diagnostic_handler& dh) -> failure_or<void> {
      auto parsed = from_string<hmac_algorithm>(args.algorithm.inner);
      if (not parsed) {
        diagnostic::error("`algorithm` must be one of "
                          "`sha256`, `sha512`, `sha384`, `sha1`, `md5`")
          .primary(args.algorithm)
          .emit(dh);
        return failure::promise();
      }
      args.parsed_algorithm = *parsed;
      return {};
    });
    return std::move(d).finish();
  }

  auto make_function(function_invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
    auto data = ast::expression{};
    auto key = ast::expression{};
    auto algorithm = located<std::string>{"sha256", location::unknown};
    TRY(argument_parser2::function(name())
          .positional("data", data, "any")
          .positional("key", key, "string")
          .named_optional("algorithm", algorithm)
          .parse(inv, ctx));
    auto parsed_algorithm = from_string<hmac_algorithm>(algorithm.inner);
    if (not parsed_algorithm) {
      diagnostic::error("`algorithm` must be one of "
                        "`sha256`, `sha512`, `sha384`, `sha1`, `md5`")
        .primary(algorithm)
        .emit(ctx);
      return failure::promise();
    }
    return function_use::make([data = std::move(data), key = std::move(key),
                               algorithm = *parsed_algorithm](evaluator eval,
                                                              session ctx) {
      return map_series(
        eval(data), eval(key),
        [&](series data_values, series key_values) -> multi_series {
          TENZIR_ASSERT(data_values.length() == key_values.length());
          auto builder = arrow::StringBuilder{tenzir::arrow_memory_pool()};
          check(builder.Reserve(data_values.length()));
          match(
            std::tie(*data_values.array, *key_values.array),
            [&]<class DataArray>(DataArray const& data_array,
                                 arrow::StringArray const& key_array) {
              for (auto i = int64_t{0}; i < data_array.length(); ++i) {
                if (key_array.IsNull(i)) {
                  check(builder.AppendNull());
                  continue;
                }
                auto value
                  = view_at(static_cast<arrow::Array const&>(data_array), i);
                if (is<caf::none_t>(value)) {
                  check(builder.AppendNull());
                  continue;
                }
                auto digest = compute_hmac(
                  value, as_bytes(key_array.GetView(i)), algorithm);
                check(builder.Append(digest));
              }
            },
            [&]<class DataArray>(DataArray const& data_array,
                                 arrow::NullArray const&) {
              check(builder.AppendNulls(data_array.length()));
            },
            [&]<class DataArray, class KeyArray>(DataArray const& data_array,
                                                 KeyArray const&) -> void {
              if constexpr (not detail::is_any_v<KeyArray, arrow::NullArray>) {
                diagnostic::warning("expected `string`, but got `{}`",
                                    key_values.type.kind())
                  .primary(key)
                  .emit(ctx);
              }
              check(builder.AppendNulls(data_array.length()));
            });
          return series{string_type{}, finish(builder)};
        });
    });
  }
};

} // namespace

} // namespace tenzir::plugins::hash

TENZIR_REGISTER_PLUGIN(tenzir::plugins::hash::hmac)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::hash::fun<tenzir::md5, "md5">)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::hash::fun<tenzir::sha1, "sha1">)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::hash::fun<tenzir::sha224, "sha224">)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::hash::fun<tenzir::sha256, "sha256">)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::hash::fun<tenzir::sha384, "sha384">)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::hash::fun<tenzir::sha512, "sha512">)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::hash::fun<tenzir::sha3_224, "sha3_224">)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::hash::fun<tenzir::sha3_256, "sha3_256">)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::hash::fun<tenzir::sha3_384, "sha3_384">)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::hash::fun<tenzir::sha3_512, "sha3_512">)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::hash::fun<tenzir::xxh3_64, "xxh3">)
