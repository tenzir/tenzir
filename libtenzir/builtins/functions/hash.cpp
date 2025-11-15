//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/argument_parser2.hpp>
#include <tenzir/arrow_table_slice.hpp>
#include <tenzir/arrow_utils.hpp>
#include <tenzir/as_bytes.hpp>
#include <tenzir/concept/convertible/data.hpp>
#include <tenzir/concept/convertible/to.hpp>
#include <tenzir/concept/parseable/core.hpp>
#include <tenzir/concept/parseable/tenzir/option_set.hpp>
#include <tenzir/concept/parseable/tenzir/pipeline.hpp>
#include <tenzir/cast.hpp>
#include <tenzir/detail/base64.hpp>
#include <tenzir/detail/coding.hpp>
#include <tenzir/detail/inspection_common.hpp>
#include <tenzir/detail/narrow.hpp>
#include <tenzir/error.hpp>
#include <tenzir/hash/hash_append.hpp>
#include <tenzir/hash/md5.hpp>
#include <tenzir/hash/sha.hpp>
#include <tenzir/hash/xxhash.hpp>
#include <tenzir/location.hpp>
#include <tenzir/optional.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/secret.hpp>
#include <tenzir/tql2/eval.hpp>
#include <tenzir/tql2/plugin.hpp>

#include <arrow/scalar.h>
#include <fmt/format.h>

#include <string_view>

namespace tenzir::plugins::hash {

namespace hash_detail {

auto flatten_secret(const secret_view& secret, session ctx, location where,
                    std::string_view function_name)
  -> std::optional<std::string> {
  auto& dh = ctx.dh();
  struct visitor {
    diagnostic_handler& dh;
    location where;
    std::string_view function_name;

    auto operator()(const fbs::data::SecretLiteral& lit) const
      -> std::optional<std::string> {
      const auto value = detail::secrets::deref(lit.value()).string_view();
      return std::string{value};
    }

    auto operator()(const fbs::data::SecretName& name) const
      -> std::optional<std::string> {
      diagnostic::error("`{}` requires literal secrets; got managed secret "
                        "`{}`",
                        function_name, name.value()->string_view())
        .primary(where)
        .emit(dh);
      return std::nullopt;
    }

    auto operator()(const fbs::data::SecretConcatenation& concat) const
      -> std::optional<std::string> {
      auto result = std::string{};
      for (const auto* child : detail::secrets::deref(concat.secrets())) {
        auto flattened = match(detail::secrets::deref(child), *this);
        if (not flattened) {
          return std::nullopt;
        }
        result += std::move(*flattened);
      }
      return result;
    }

    auto operator()(const fbs::data::SecretTransformed& transformed) const
      -> std::optional<std::string> {
      auto inner = match(detail::secrets::deref(transformed.secret()), *this);
      if (not inner) {
        return std::nullopt;
      }
      using enum fbs::data::SecretTransformations;
      switch (transformed.transformation()) {
        case encode_base64:
          return detail::base64::encode(*inner);
        case decode_base64: {
          auto decoded = detail::base64::try_decode(std::string_view{*inner});
          if (not decoded) {
            diagnostic::error("`{}` failed to decode base64 secret value",
                              function_name)
              .primary(where)
              .emit(dh);
            return std::nullopt;
          }
          return std::string{decoded->begin(), decoded->end()};
        }
      }
      TENZIR_UNREACHABLE();
    }
  };

  return match(*secret.buffer, visitor{dh, where, function_name});
}

} // namespace hash_detail

namespace {

using hash_detail::flatten_secret;

/// The configuration of the hash pipeline operator.
struct configuration {
  std::string field;
  std::string out;
  std::optional<std::string> salt;

  /// Support type inspection for easy parsing with convertible.
  template <class Inspector>
  friend auto inspect(Inspector& f, configuration& x) {
    return detail::apply_all(f, x.field, x.out, x.salt);
  }

  /// Enable parsing from a record via convertible.
  static inline const record_type& schema() noexcept {
    static auto result = record_type{
      {"field", string_type{}},
      {"out", string_type{}},
      {"salt", string_type{}},
    };
    return result;
  }
};

class hash_operator final
  : public schematic_operator<hash_operator,
                              std::vector<indexed_transformation>> {
public:
  hash_operator() = default;

  explicit hash_operator(configuration configuration)
    : config_(std::move(configuration)) {
  }

  auto initialize(const type& schema, operator_control_plane&) const
    -> caf::expected<state_type> override {
    // Get the target field if it exists.
    auto column_index = schema.resolve_key_or_concept_once(config_.field);
    if (! column_index) {
      return state_type{};
    }
    auto transform_fn = [this](struct record_type::field field,
                               std::shared_ptr<arrow::Array> array) noexcept
      -> std::vector<
        std::pair<struct record_type::field, std::shared_ptr<arrow::Array>>> {
      auto hashes_builder
        = string_type::make_arrow_builder(arrow_memory_pool());
      if (config_.salt) {
        for (const auto& value : values(field.type, *array)) {
          const auto digest = tenzir::hash(value, *config_.salt);
          const auto append_result
            = hashes_builder->Append(fmt::format("{:x}", digest));
          TENZIR_ASSERT(append_result.ok(), append_result.ToString().c_str());
        }
      } else {
        for (const auto& value : values(field.type, *array)) {
          const auto digest = tenzir::hash(value);
          const auto append_result
            = hashes_builder->Append(fmt::format("{:x}", digest));
          TENZIR_ASSERT(append_result.ok(), append_result.ToString().c_str());
        }
      }
      return {
        {
          std::move(field),
          std::move(array),
        },
        {
          {
            config_.out,
            string_type{},
          },
          finish(*hashes_builder),
        },
      };
    };
    return state_type{{*column_index, std::move(transform_fn)}};
  }

  auto process(table_slice slice, state_type& state) const
    -> output_type override {
    return transform_columns(slice, state);
  };

  auto name() const noexcept -> std::string override {
    return "hash";
  }

  auto optimize(expression const& filter, event_order order) const
    -> optimize_result override {
    (void)filter;
    return optimize_result::order_invariant(*this, order);
  }

  friend auto inspect(auto& f, hash_operator& x) -> bool {
    return f.apply(x.config_);
  }

private:
  /// The underlying configuration of the transformation.
  configuration config_ = {};
};

class plugin final : public virtual operator_plugin<hash_operator> {
public:
  auto signature() const -> operator_signature override {
    return {.transformation = true};
  }

  auto make_operator(std::string_view pipeline) const
    -> std::pair<std::string_view, caf::expected<operator_ptr>> override {
    using parsers::end_of_pipeline_operator, parsers::required_ws_or_comment,
      parsers::optional_ws_or_comment, parsers::extractor_list;
    const auto* f = pipeline.begin();
    const auto* const l = pipeline.end();
    const auto options = option_set_parser{{{"salt", 's'}}};
    const auto option_parser = (required_ws_or_comment >> options);
    auto parsed_options = std::unordered_map<std::string, data>{};
    if (! option_parser(f, l, parsed_options)) {
      return {
        std::string_view{f, l},
        caf::make_error(ec::syntax_error, fmt::format("failed to parse hash "
                                                      "operator options: '{}'",
                                                      pipeline)),
      };
    }
    const auto extractor_parser = optional_ws_or_comment >> extractor_list
                                  >> optional_ws_or_comment
                                  >> end_of_pipeline_operator;
    auto parsed_extractors = std::vector<std::string>{};
    if (! extractor_parser(f, l, parsed_extractors)) {
      return {
        std::string_view{f, l},
        caf::make_error(ec::syntax_error, fmt::format("failed to parse hash "
                                                      "operator extractor: "
                                                      "'{}'",
                                                      pipeline)),
      };
    }
    auto config = configuration{};
    config.field = parsed_extractors.front();
    config.out = parsed_extractors.front() + "_hashed";
    for (const auto& [key, value] : parsed_options) {
      auto value_str = try_as<std::string>(&value);
      if (! value_str) {
        return {
          std::string_view{f, l},
          caf::make_error(ec::syntax_error, fmt::format("invalid option value "
                                                        "string for "
                                                        "pseudonymize "
                                                        "operator: "
                                                        "'{}'",
                                                        value)),
        };
      }
      if (key == "s" || key == "salt") {
        config.salt = *value_str;
      }
    }
    return {
      std::string_view{f, l},
      std::make_unique<hash_operator>(std::move(config)),
    };
  }
};

} // namespace

template <class HashAlgorithm, detail::string_literal Name>
class fun : public virtual function_plugin {
  auto name() const -> std::string override {
    return fmt::format("hash_{}", Name);
  }

  auto is_deterministic() const -> bool override {
    return true;
  }

  auto make_function(invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
    auto expr = ast::expression{};
    auto seed = std::optional<std::string>{};
    TRY(argument_parser2::function(name())
          .positional("x", expr, "any")
          .named("seed", seed)
          .parse(inv, ctx));
    return function_use::make(
      [expr_ = std::move(expr), seed_ = std::move(seed)](evaluator eval,
                                                         session) -> series {
        const auto& s = eval(expr_);
        auto hash = [&](const auto& x) {
          // We only hash the bytes and the length. Users expect that the
          // resulting digest is the same as in other tools, which hash the
          // sequence of bytes. This includes hashing the seed.
          HashAlgorithm hasher{};
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
          };
          match(x, f);
          return std::move(hasher).finish();
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

} // namespace tenzir::plugins::hash

namespace tenzir::plugins::hmac {

template <class HmacAlgorithm, detail::string_literal Name>
class fun : public virtual function_plugin {
public:
  auto name() const -> std::string override {
    return fmt::format("hmac_{}", Name);
  }

  auto is_deterministic() const -> bool override {
    return true;
  }

  auto make_function(invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
    auto value_expr = ast::expression{};
    auto key = located<secret>{};
    TRY(argument_parser2::function(name())
          .positional("value", value_expr, "any")
          .positional("key", key, "secret")
          .parse(inv, ctx));
    const auto fn_name = std::string{name()};
    auto key_material = hash::hash_detail::flatten_secret(
      secret_view{key.inner}, ctx, key.source, fn_name);
    if (not key_material) {
      return failure::promise();
    }
    auto key_bytes = std::string{*key_material};
    return function_use::make(
      [value_expr = std::move(value_expr), key_bytes = std::move(key_bytes)](
        evaluator eval, session ctx) -> multi_series {
        (void)ctx;
        auto compute = [&, key_bytes = std::string_view{key_bytes}](
                         series value) -> series {
          auto builder = string_type::make_arrow_builder(arrow_memory_pool());
          for (auto row = int64_t{0}; row < value.length(); ++row) {
            if (value.array->IsNull(row)) {
              check(builder->AppendNull());
              continue;
            }
            auto hasher = HmacAlgorithm{as_bytes(key_bytes)};
            auto append_value = detail::overload{
              [&](const auto& data) {
                hash_append(hasher, data);
              },
              [&](std::string_view str) {
                hasher.add(as_bytes(str));
              },
            };
            match(value_at(value.type, *value.array, row), append_value);
            auto digest = std::move(hasher).finish();
            auto hex = detail::hexify(as_bytes(digest));
            check(builder->Append(hex));
          }
          return series{string_type{}, finish(*builder)};
        };
        return map_series(eval(value_expr),
                          [&](series value) -> multi_series {
                            return multi_series{compute(std::move(value))};
                          });
      });
  }
};

} // namespace tenzir::plugins::hmac

TENZIR_REGISTER_PLUGIN(tenzir::plugins::hash::plugin)
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
TENZIR_REGISTER_PLUGIN(tenzir::plugins::hmac::fun<tenzir::hmac_md5, "md5">)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::hmac::fun<tenzir::hmac_sha1, "sha1">)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::hmac::fun<tenzir::hmac_sha224, "sha22"
                                                                       "4">)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::hmac::fun<tenzir::hmac_sha256, "sha25"
                                                                       "6">)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::hmac::fun<tenzir::hmac_sha384, "sha38"
                                                                       "4">)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::hmac::fun<tenzir::hmac_sha512, "sha51"
                                                                       "2">)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::hmac::fun<tenzir::hmac_sha3_224, "sha3_"
                                                                         "224">)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::hmac::fun<tenzir::hmac_sha3_256, "sha3_"
                                                                         "256">)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::hmac::fun<tenzir::hmac_sha3_384, "sha3_"
                                                                         "384">)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::hmac::fun<tenzir::hmac_sha3_512, "sha3_"
                                                                         "512">)
