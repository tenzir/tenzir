//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/argument_parser2.hpp"

#include "tenzir/detail/assert.hpp"
#include "tenzir/detail/enumerate.hpp"
#include "tenzir/detail/similarity.hpp"
#include "tenzir/detail/type_traits.hpp"
#include "tenzir/tql2/eval.hpp"
#include "tenzir/tql2/exec.hpp"

#include <boost/algorithm/string.hpp>

#include <string_view>

namespace tenzir {

namespace {

template <class T>
concept data_type = detail::tl_contains_v<data::types, T>;

} // namespace

auto argument_parser2::parse(const operator_factory_plugin::invocation& inv,
                             session ctx) -> failure_or<void> {
  TENZIR_ASSERT(kind_ == kind::op);
  return parse(inv.self, inv.args, ctx);
}

auto argument_parser2::parse(const function_plugin::invocation& inv,
                             session ctx) -> failure_or<void> {
  TENZIR_ASSERT(kind_ == kind::fn);
  return parse(inv.call.fn, inv.call.args, ctx);
}

auto argument_parser2::parse(const ast::function_call& call, session ctx)
  -> failure_or<void> {
  TENZIR_ASSERT(kind_ == kind::fn);
  return parse(call.fn, call.args, ctx);
}

auto argument_parser2::parse(const ast::entity& self,
                             std::span<ast::expression const> args, session ctx)
  -> failure_or<void> {
  // TODO: Simplify and deduplicate everything in this function.
  auto result = failure_or<void>{};
  auto emit = [&](diagnostic_builder d) {
    if (d.inner().severity == severity::error) {
      result = failure::promise();
    }
    std::move(d)
      .usage(usage())
      .compose([&](auto d) {
        if (name_.starts_with('_')) {
          return d;
        }
        return std::move(d).docs(docs());
      })
      .emit(ctx);
  };
  auto kind = [](const data& x) -> std::string_view {
    // TODO: Refactor this.
    return match(x, []<class Data>(const Data&) -> std::string_view {
      if constexpr (caf::detail::is_one_of<Data, pattern>::value) {
        TENZIR_UNREACHABLE();
      } else {
        return to_string(type_kind::of<data_to_type_t<Data>>);
      }
    });
  };
  auto arg = args.begin();
  auto positional_idx = size_t{0};
  for (auto it = positional_.begin(); it != positional_.end();
       ++it, ++positional_idx) {
    auto& positional = *it;
    auto end_of_positional = arg == args.end() or is<ast::assignment>(*arg);
    if (end_of_positional) {
      if (first_optional_ && positional_idx >= *first_optional_) {
        break;
      }
      emit(diagnostic::error("expected additional positional argument `{}`",
                             positional.name)
             .primary(self));
      break;
    }
    auto& expr = *arg;
    positional.set.match(
      [&]<data_type T>(setter<located<T>>& set) {
        auto value = const_eval(expr, ctx);
        if (not value) {
          result = value.error();
          return;
        }
        // TODO: Make this more beautiful.
        auto storage = T{};
        auto cast = try_as<T>(&*value);
        if constexpr (std::same_as<T, uint64_t>) {
          if (not cast) {
            auto other = try_as<int64_t>(&*value);
            if (other) {
              if (*other < 0) {
                emit(diagnostic::error("expected positive integer, got `{}`",
                                       *other)
                       .primary(expr));
                return;
              }
              storage = *other;
              cast = &storage;
            }
          }
        }
        if (not cast) {
          emit(diagnostic::error("expected argument of type `{}`, but got `{}`",
                                 type_kind::of<data_to_type_t<T>>, kind(*value))
                 .primary(expr));
          return;
        }
        set(located{std::move(*cast), expr.get_location()});
      },
      [&](setter<located<data>>& set) {
        auto value = const_eval(expr, ctx);
        if (not value) {
          result = value.error();
          return;
        }
        set(located{std::move(*value), expr.get_location()});
      },
      [&](setter<ast::expression>& set) {
        set(expr);
      },
      [&](setter<ast::simple_selector>& set) {
        auto sel = ast::simple_selector::try_from(expr);
        if (not sel) {
          emit(diagnostic::error("expected a selector").primary(expr));
          return;
        }
        set(std::move(*sel));
      },
      [&](setter<located<pipeline>>& set) {
        auto pipe_expr = std::get_if<ast::pipeline_expr>(&*expr.kind);
        if (not pipe_expr) {
          emit(
            diagnostic::error("expected a pipeline expression").primary(expr));
          return;
        }
        auto pipe = compile(std::move(pipe_expr->inner), ctx);
        if (pipe.is_error()) {
          result = pipe.error();
          return;
        }
        set(located{std::move(pipe).unwrap(), expr.get_location()});
      });
    ++arg;
  }
  for (; arg != args.end(); ++arg) {
    arg->match(
      [&](ast::assignment assignment) {
        auto sel = std::get_if<ast::simple_selector>(&assignment.left);
        if (not sel || sel->has_this() || sel->path().size() != 1) {
          emit(diagnostic::error("invalid name").primary(assignment.left));
          return;
        }
        auto& name = sel->path()[0].name;
        auto it = std::ranges::find(named_, name, &named_t::name);
        if (it == named_.end()) {
          auto filtered = std::views::filter(named_, [](auto&& x) {
            return not x.name.starts_with("_");
          });
          if (not filtered.empty()) {
            const auto best = std::ranges::max(filtered, {}, [&](auto&& x) {
              return detail::calculate_similarity(name, x.name);
            });
            if (detail::calculate_similarity(name, best.name) > -10) {
              emit(diagnostic::error("named argument `{}` does not exist", name)
                     .primary(assignment.left)
                     .hint("did you mean `{}`?", best.name));
              return;
            }
          }
          emit(diagnostic::error("named argument `{}` does not exist", name)
                 .primary(assignment.left));
          return;
        }
        if (it->found) {
          emit(diagnostic::error("duplicate named argument `{}`", name)
                 .primary(*it->found)
                 .primary(arg->get_location()));
          return;
        }
        it->found = arg->get_location();
        auto& expr = assignment.right;
        it->set.match(
          [&]<data_type T>(setter<located<T>>& set) {
            auto value = const_eval(expr, ctx);
            if (not value) {
              result = value.error();
              return;
            }
            auto cast = try_as<T>(&*value);
            if constexpr (std::same_as<T, uint64_t>) {
              if (not cast) {
                auto other = try_as<int64_t>(&*value);
                if (other) {
                  if (*other < 0) {
                    emit(diagnostic::error(
                           "expected positive integer, got `{}`", *other)
                           .primary(expr));
                    return;
                  }
                  value = static_cast<uint64_t>(*other);
                  cast = try_as<T>(&*value);
                }
              }
            }
            if (not cast) {
              // TODO: Attempt conversion.
              emit(diagnostic::error(
                     "expected argument of type `{}`, but got `{}`",
                     type_kind::of<data_to_type_t<T>>, kind(*value))
                     .primary(expr));
              return;
            }
            set(located{std::move(*cast), expr.get_location()});
          },
          [&](setter<located<data>>& set) {
            auto value = const_eval(expr, ctx);
            if (not value) {
              result = value.error();
              return;
            }
            set(located{std::move(*value), expr.get_location()});
          },
          [&](setter<ast::expression>& set) {
            set(expr);
          },
          [&](setter<ast::simple_selector>& set) {
            auto sel = ast::simple_selector::try_from(expr);
            if (not sel) {
              emit(diagnostic::error("expected a selector").primary(expr));
              return;
            }
            set(std::move(*sel));
          },
          [&](setter<located<pipeline>>& set) {
            auto pipe_expr = std::get_if<ast::pipeline_expr>(&*expr.kind);
            if (not pipe_expr) {
              emit(diagnostic::error("expected a pipeline expression")
                     .primary(expr));
              return;
            }
            auto pipe = compile(std::move(pipe_expr->inner), ctx);
            if (pipe.is_error()) {
              result = pipe.error();
              return;
            }
            set(located{std::move(pipe).unwrap(), expr.get_location()});
          });
      },
      [&](ast::pipeline_expr pipe_expr) {
        if (positional_idx == positional_.size()) {
          emit(diagnostic::error("did not expect more positional arguments")
                 .primary(*arg));
          return;
        }
        positional_[positional_idx].set.match(
          [&](setter<located<pipeline>>& set) {
            auto pipe = compile(std::move(pipe_expr.inner), ctx);
            if (pipe.is_error()) {
              result = pipe.error();
              return;
            }
            set(located{std::move(pipe).unwrap(), pipe_expr.get_location()});
          },
          [&](auto&) {
            TENZIR_UNREACHABLE();
          });
        ++positional_idx;
      },
      [&](auto&) {
        if (positional_idx == positional_.size()) {
          emit(diagnostic::error("unexpected argument").primary(*arg));
        }
      });
  }
  for (const auto& arg : named_) {
    if (arg.required and not arg.found) {
      emit(
        diagnostic::error("required argument `{}` was not provided", arg.name)
          .primary(self.get_location()));
    }
  }
  return result;
}

auto argument_parser2::usage() const -> std::string {
  enum class pipeline_last_t { none, req, opt };
  auto setter_to_string = [](const any_setter& set) {
    return set.match(
      []<data_type T>(const setter<located<T>>&) -> std::string {
        if constexpr (std::same_as<T, std::string>) {
          return "string";
        } else if constexpr (concepts::one_of<T, uint64_t, int64_t>) {
          return "int";
        } else if constexpr (concepts::one_of<T, double>) {
          return "number";
        } else {
          return fmt::format("{}", type_kind::of<data_to_type_t<T>>);
        }
      },
      [](const setter<located<data>>&) -> std::string {
        return "any";
      },
      [](const setter<ast::expression>&) -> std::string {
        // TODO: This might not be what we want. Perhaps we make this
        // customizable instead.
        return "any";
      },
      [](const setter<ast::simple_selector>&) -> std::string {
        // TODO: `field` is not 100% accurate, but we use it in the docs.
        return "field";
      },
      [](const setter<located<pipeline>>&) -> std::string {
        return "{ … }";
      });
  };
  if (usage_cache_.empty()) {
    usage_cache_ += name_;
    usage_cache_ += kind_ == kind::op ? ' ' : '(';
    auto has_previous = false;
    auto in_brackets = false;
    auto pipeline_last = pipeline_last_t::none;
    for (auto [idx, positional] : detail::enumerate(positional_)) {
      auto last = idx == positional_.size() - 1;
      auto is_pipeline
        = std::holds_alternative<setter<located<pipeline>>>(positional.set);
      auto is_optional = first_optional_ && idx >= *first_optional_;
      if (last && is_pipeline && kind_ == kind::op) {
        // We want to print named arguments before, so we skip this for now.
        pipeline_last
          = is_optional ? pipeline_last_t::opt : pipeline_last_t::req;
        continue;
      }
      if (std::exchange(has_previous, true)) {
        usage_cache_ += ", ";
      }
      if (is_optional) {
        if (not in_brackets) {
          usage_cache_ += '[';
          in_brackets = true;
        }
      } else {
        TENZIR_ASSERT(not in_brackets);
      }
      auto type = positional.type.empty() ? setter_to_string(positional.set)
                                          : positional.type;
      usage_cache_ += fmt::format("{}:{}", positional.name, type);
    }
    const auto append_named_option = [&](const named_t& opt) {
      if (opt.name.starts_with("_")) {
        // This denotes an internal/unstable option.
        return;
      }
      if (opt.required and in_brackets) {
        usage_cache_ += ']';
        in_brackets = false;
      }
      if (std::exchange(has_previous, true)) {
        usage_cache_ += ", ";
      }
      if (not opt.required and not in_brackets) {
        usage_cache_ += '[';
        in_brackets = true;
      }
      auto type = opt.type.empty() ? setter_to_string(opt.set) : opt.type;
      usage_cache_ += fmt::format("{}={}", opt.name, type);
    };
    for (const auto& opt : named_) {
      if (opt.required) {
        append_named_option(opt);
      }
    }
    for (const auto& opt : named_) {
      if (not opt.required) {
        append_named_option(opt);
      }
    }
    if (pipeline_last != pipeline_last_t::none) {
      auto is_optional = pipeline_last == pipeline_last_t::opt;
      if (is_optional and not in_brackets) {
        usage_cache_ += '[';
        in_brackets = true;
      } else if (not is_optional and in_brackets) {
        usage_cache_ += ']';
        in_brackets = false;
      }
      if (std::exchange(has_previous, true)) {
        usage_cache_ += " ";
      }
      usage_cache_ += "{ … }";
    }
    if (in_brackets) {
      usage_cache_ += ']';
      in_brackets = false;
    }
    if (kind_ != kind::op) {
      usage_cache_ += ')';
    }
  }
  return usage_cache_;
}

auto argument_parser2::docs() const -> std::string {
  auto name = name_;
  auto category = std::invoke([&] {
    switch (kind_) {
      case kind::op:
        return "operators";
      case kind::fn:
        return "functions";
    }
    TENZIR_UNREACHABLE();
  });
  boost::replace_all(name, "::", "/");
  return fmt::format("https://docs.tenzir.com/tql2/{}/{}", category, name);
}

template <class T>
auto argument_parser2::make_setter(T& x) -> auto {
  using value_type = decltype(std::invoke([] {
    if constexpr (detail::is_specialization_of<std::optional, T>::value) {
      return tag_v<typename T::value_type>;
    } else {
      return tag_v<T>;
    }
  }))::type;
  if constexpr (std::same_as<T, std::optional<location>>) {
    return setter<located<bool>>{[&x](located<bool> y) {
      if (y.inner) {
        x = y.source;
      } else {
        x = std::nullopt;
      }
    }};
  } else if constexpr (argument_parser_bare_type<value_type>) {
    return setter<located<value_type>>{[&x](located<value_type> y) {
      x = std::move(y.inner);
    }};
  } else {
    return setter<value_type>{[&x](value_type y) {
      x = std::move(y);
    }};
  }
}

template <argument_parser_type T>
auto argument_parser2::positional(std::string name, T& x, std::string type)
  -> argument_parser2& {
  TENZIR_ASSERT(not first_optional_, "encountered required positional after "
                                     "optional positional argument");
  positional_.emplace_back(std::move(name), std::move(type), make_setter(x));
  return *this;
}

template <argument_parser_type T>
auto argument_parser2::positional(std::string name, std::optional<T>& x,
                                  std::string type) -> argument_parser2& {
  if (not first_optional_) {
    first_optional_ = positional_.size();
  }
  positional_.emplace_back(std::move(name), std::move(type), make_setter(x));
  return *this;
}

template <argument_parser_type T>
auto argument_parser2::named(std::string name, T& x, std::string type)
  -> argument_parser2& {
  named_.emplace_back(std::move(name), std::move(type), make_setter(x), true);
  return *this;
}

template <argument_parser_type T>
auto argument_parser2::named(std::string name, std::optional<T>& x,
                             std::string type) -> argument_parser2& {
  named_.emplace_back(std::move(name), std::move(type), make_setter(x), false);
  return *this;
}

template <argument_parser_type T>
auto argument_parser2::named_optional(std::string name, T& x, std::string type)
  -> argument_parser2& {
  named_.emplace_back(std::move(name), std::move(type), make_setter(x), false);
  return *this;
}

auto argument_parser2::named(std::string name, std::optional<location>& x,
                             std::string type) -> argument_parser2& {
  named_.emplace_back(std::move(name), std::move(type), make_setter(x), false);
  return *this;
}

auto argument_parser2::named(std::string name, bool& x, std::string type)
  -> argument_parser2& {
  named_.emplace_back(std::move(name), std::move(type), make_setter(x), false);
  return *this;
}

template <std::monostate>
struct instantiate_argument_parser_methods {
  template <class T>
  using func = auto (argument_parser2::*)(std::string, T&, std::string)
    -> argument_parser2&;

  template <class... T>
  struct inner {
    static constexpr auto value = std::tuple{
      static_cast<func<T>>(&argument_parser2::positional)...,
      static_cast<func<std::optional<T>>>(&argument_parser2::positional)...,
      static_cast<func<T>>(&argument_parser2::named_optional)...,
      static_cast<func<T>>(&argument_parser2::named)...,
      static_cast<func<std::optional<T>>>(&argument_parser2::named)...,
    };
  };

  static constexpr auto value
    = detail::tl_apply_t<argument_parser_types, inner>::value;
};

template struct instantiate_argument_parser_methods<std::monostate{}>;

auto check_no_substrings(diagnostic_handler& dh,
                         std::vector<argument_info> values)
  -> failure_or<void> {
  for (size_t i = 0; i < values.size(); ++i) {
    for (size_t j = i + 1; j < values.size(); ++j) {
      const auto i_larger = values[i].value.size() > values[j].value.size();
      const auto& longer = i_larger ? values[i] : values[j];
      const auto& shorter = i_larger ? values[j] : values[i];
      if (shorter.value.empty()) {
        continue;
      }
      if (longer.value.find(shorter.value) != longer.value.npos) {
        diagnostic::error("`{}` and `{}` conflict", shorter.name, longer.name)
          .note("`{}` is a substring of `{}`", shorter.value, longer.value)
          .primary(shorter.loc)
          .primary(longer.loc)
          .emit(dh);
        return failure::promise();
      }
    }
  }
  return {};
}

auto check_non_empty(std::string_view name, const located<std::string>& v,
                     diagnostic_handler& dh) -> failure_or<void> {
  if (v.inner.empty()) {
    diagnostic::error("`{}` must not be empty", name).primary(v).emit(dh);
    return failure::promise();
  }
  return {};
}

} // namespace tenzir
