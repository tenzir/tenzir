//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/argument_parser.hpp>
#include <tenzir/pipeline.hpp>
#include <tenzir/plugin.hpp>

#include <arrow/api.h>

#include <ranges>
#include <string_view>

namespace tenzir::plugins::yield {

namespace {

struct unnest {};

using projection = located<variant<std::string, unnest>>;

constexpr auto unnest_idx = std::numeric_limits<offset::value_type>::max();

class yield_operator final
  : public schematic_operator<yield_operator,
                              std::optional<std::pair<offset, type>>> {
public:
  yield_operator() = default;

  explicit yield_operator(parser_interface& p) {
    auto parser = argument_parser{"yield", "https://docs.tenzir.com/"
                                           "operators/yield"};
    auto extractor = located<std::string>{};
    // TODO: This assumes that the given extractor can be parsed as a shell-like
    // argument, e.g., spaces must be quoted.
    parser.add(extractor, "<extractor>");
    parser.parse(p);
    // TODO: The locations reported by the parsed can be slightly wrong if the
    // argument is quoted. This can be resolved as part of the expression revamp.
    auto field = +(parsers::alnum | parsers::chr{'_'});
    auto current = extractor.inner.begin();
    auto end = extractor.inner.end();
    auto to_offset = [&](std::string::iterator it, int64_t x = 0) -> size_t {
      if (not extractor.source) {
        return 0;
      }
      return extractor.source.begin + (it - extractor.inner.begin()) + x;
    };
    auto parse_field = [&]() -> projection {
      auto last = current;
      if (not field(current, end, unused)) {
        diagnostic::error("expected field name")
          .primary(tenzir::location{extractor.source.source, to_offset(current),
                                    to_offset(current, 1)})
          .throw_();
      }
      return projection{std::string{last, current},
                        {extractor.source.source, to_offset(last),
                         to_offset(current)}};
    };
    path.emplace_back(parse_field());
    while (current != end) {
      if (*current == '.') {
        ++current;
        path.push_back(parse_field());
      } else if (*current == '[') {
        ++current;
        if (*current == ']') {
          ++current;
          path.emplace_back(unnest{}, tenzir::location{extractor.source.source,
                                                       to_offset(current, -2),
                                                       to_offset(current)});
        } else {
          diagnostic::error("expected `]`")
            .primary(tenzir::location{extractor.source.source,
                                      to_offset(current),
                                      to_offset(current, 1)})
            .throw_();
        }
      } else {
        diagnostic::error("expected `.<field>` or `[]`")
          .primary(tenzir::location{extractor.source.source, to_offset(current),
                                    to_offset(current, 1)})
          .throw_();
      }
    }
  }

  auto initialize(const type& schema, operator_control_plane& ctrl) const
    -> caf::expected<state_type> override {
    auto current = schema;
    auto result = offset{};
    for (auto& proj : path) {
      auto success = proj.inner.match(
        [&](const std::string& field) {
        auto rec_ty = caf::get_if<record_type>(&current);
        if (not rec_ty) {
          diagnostic::warning("expected a record, but got a {}", current.kind())
            .primary(proj.source)
            .note("for schema `{}`", schema)
            .emit(ctrl.diagnostics());
          return false;
        }
        auto index = rec_ty->resolve_key(field);
        if (not index) {
          diagnostic::warning("record has no field `{}`", field)
            .primary(proj.source)
            .hint("must be one of: {}",
                  fmt::join(
                    rec_ty->fields()
                      | std::views::transform(&record_type::field_view::name),
                    ", "))
            .note("for schema `{}`", schema)
            .emit(ctrl.diagnostics());
          return false;
        }
        TENZIR_ASSERT(index->size() == 1);
        current = rec_ty->field(*index).type;
        result.push_back((*index)[0]);
        return true;
      },
        [&](unnest) {
        auto list_ty = caf::get_if<list_type>(&current);
        if (not list_ty) {
          diagnostic::warning("expected a list, but got a {}", current.kind())
            .primary(proj.source)
            .note("for schema `{}`", schema)
            .emit(ctrl.diagnostics());
          return false;
        }
        current = list_ty->value_type();
        result.push_back(unnest_idx);
        return true;
      });
      if (not success) {
        return std::nullopt;
      }
    }
    if (not caf::holds_alternative<record_type>(current)) {
      diagnostic::warning("expected a record, but got a {}", current.kind())
        .primary(path.back().source)
        .note("for schema `{}`", schema)
        .emit(ctrl.diagnostics());
      return std::nullopt;
    }
    return std::pair{std::move(result), type{"tenzir.yield", current}};
  }

  auto process(table_slice slice, state_type& state) const
    -> output_type override {
    // TODO: We currently resolve fields of `null` records as `null`. If a list
    // is `null`, then we interpret it as the empty list. We should reconsider
    // this behavior and whether we want to emit diagnostics when we decide how
    // nulls should be handled in general.
    if (not state.has_value()) {
      return {};
    }
    auto batch = to_record_batch(slice);
    auto array = std::static_pointer_cast<arrow::Array>(
      batch->ToStructArray().ValueOrDie());
    auto& [indices, new_type] = *state;
    for (auto index : indices) {
      TENZIR_ASSERT(array);
      if (index == unnest_idx) {
        auto list_array = dynamic_cast<arrow::ListArray*>(array.get());
        TENZIR_ASSERT(list_array);
        array = list_array->Flatten().ValueOrDie();
      } else {
        auto struct_array = dynamic_cast<arrow::StructArray*>(array.get());
        TENZIR_ASSERT(struct_array);
        array = struct_array->GetFlattenedField(detail::narrow<int>(index))
                  .ValueOrDie();
      }
    }
    auto record = std::dynamic_pointer_cast<arrow::StructArray>(array);
    TENZIR_ASSERT(record);
    auto fields = record->Flatten().ValueOrDie();
    auto result
      = table_slice{arrow::RecordBatch::Make(new_type.to_arrow_schema(),
                                             array->length(), fields),
                    new_type};
    TENZIR_ASSERT_EXPENSIVE(to_record_batch(result)->Validate().ok());
    return result;
  }

  auto name() const -> std::string override {
    return "yield";
  }

  auto optimize(expression const& filter, event_order order) const
    -> optimize_result override {
    (void)filter;
    return optimize_result::order_invariant(*this, order);
  }

  friend auto inspect(auto& f, yield_operator& x) -> bool {
    return f.apply(x.path);
  }

private:
  std::vector<projection> path;
};

class plugin final : public virtual operator_plugin<yield_operator> {
public:
  auto signature() const -> operator_signature override {
    return {.transformation = true};
  }

  auto parse_operator(parser_interface& p) const -> operator_ptr override {
    return std::make_unique<yield_operator>(p);
  }
};

} // namespace

} // namespace tenzir::plugins::yield

TENZIR_REGISTER_PLUGIN(tenzir::plugins::yield::plugin)
