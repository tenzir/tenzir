//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/aggregation_function.hpp>
#include <tenzir/fbs/aggregation.hpp>
#include <tenzir/flatbuffer.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/tql2/eval.hpp>
#include <tenzir/tql2/plugin.hpp>

namespace tenzir::plugins::collect {

namespace {

template <concrete_type Type>
class collect_function final : public aggregation_function {
public:
  explicit collect_function(type input_type) noexcept
    : aggregation_function(std::move(input_type)) {
    // nop
  }

private:
  auto output_type() const -> type override {
    return type{list_type{input_type()}};
  }

  auto add(const data_view& view) -> void override {
    using view_type = tenzir::view<type_to_data_t<Type>>;
    if (is<caf::none_t>(view)) {
      return;
    }
    result_.push_back(materialize(as<view_type>(view)));
  }

  auto add(const arrow::Array& array) -> void override {
    const auto& typed_array = as<type_to_arrow_array_t<Type>>(array);
    result_.reserve(result_.size()
                    + (typed_array.length() - typed_array.null_count()));
    for (auto&& value : values(as<Type>(input_type()), typed_array)) {
      if (not value) {
        continue;
      }
      result_.push_back(materialize(*value));
    }
  }

  auto finish() && -> caf::expected<data> override {
    return data{std::exchange(result_, {})};
  }

  list result_ = {};
};

class collect_instance final : public aggregation_instance {
public:
  explicit collect_instance(ast::expression expr) : expr_{std::move(expr)} {
  }

  auto update(const table_slice& input, session ctx) -> void override {
    auto arg = eval(expr_, input, ctx);
    if (is<null_type>(arg.type)) {
      return;
    }
    // NOTE: Currently, different types end up coerced to strings.
    for (auto i = int64_t{}; i < arg.array->length(); ++i) {
      if (arg.array->IsNull(i)) {
        continue;
      }
      result_.push_back(materialize(value_at(arg.type, *arg.array, i)));
    }
  }

  auto get() const -> data override {
    return result_;
  }

  auto save() const -> chunk_ptr override {
    auto fbb = flatbuffers::FlatBufferBuilder{};
    auto offsets = std::vector<flatbuffers::Offset<fbs::Data>>{};
    offsets.reserve(result_.size());
    for (const auto& element : result_) {
      offsets.push_back(pack(fbb, element));
    }
    const auto fb_result = fbb.CreateVector(offsets);
    const auto fb_min_max
      = fbs::aggregation::CreateCollectDistinct(fbb, fb_result);
    fbb.Finish(fb_min_max);
    return chunk::make(fbb.Release());
  }

  auto restore(chunk_ptr chunk, session ctx) -> void override {
    const auto fb
      = flatbuffer<fbs::aggregation::CollectDistinct>::make(std::move(chunk));
    if (not fb) {
      diagnostic::warning("invalid FlatBuffer")
        .note("failed to restore `collect` aggregation instance")
        .emit(ctx);
      return;
    }
    const auto* fb_result = (*fb)->result();
    if (not fb_result) {
      diagnostic::warning("missing field `result`")
        .note("failed to restore `collect` aggregation instance")
        .emit(ctx);
      return;
    }
    result_.clear();
    result_.reserve(fb_result->size());
    for (const auto* fb_element : *fb_result) {
      if (not fb_element) {
        diagnostic::warning("missing element in field `result`")
          .note("failed to restore `collect` aggregation instance")
          .emit(ctx);
        return;
      }
      auto element = data{};
      if (auto err = unpack(*fb_element, element)) {
        diagnostic::warning("{}", err)
          .note("failed to restore `collect` aggregation instance")
          .emit(ctx);
        return;
      }
      result_.push_back(std::move(element));
    }
  }

private:
  ast::expression expr_;
  list result_;
};

class plugin : public virtual aggregation_function_plugin,
               public virtual aggregation_plugin {
  auto name() const -> std::string override {
    return "collect";
  };

  auto make_aggregation_function(const type& input_type) const
    -> caf::expected<std::unique_ptr<aggregation_function>> override {
    auto f = [&]<concrete_type Type>(
               const Type&) -> std::unique_ptr<aggregation_function> {
      return std::make_unique<collect_function<Type>>(input_type);
    };
    return caf::visit(f, input_type);
  }

  auto make_aggregation(invocation inv, session ctx) const
    -> failure_or<std::unique_ptr<aggregation_instance>> override {
    auto expr = ast::expression{};
    TRY(argument_parser2::function(name()).add(expr, "<expr>").parse(inv, ctx));
    return std::make_unique<collect_instance>(std::move(expr));
  }

  auto aggregation_default() const -> data override {
    return {};
  }
};

} // namespace

} // namespace tenzir::plugins::collect

TENZIR_REGISTER_PLUGIN(tenzir::plugins::collect::plugin)
