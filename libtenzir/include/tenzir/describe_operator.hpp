// TODO: Delete me.

#include "tenzir/compile_Ctx.hpp"
#include "tenzir/ir.hpp"

namespace tenzir {

auto make_op_parser() -> ir::operator_ptr;

struct operator_description {
  template <class... Args>
  explicit operator_description(Args...) {
  }
};

template <class T>
struct operator_description_builder {
  auto positional(std::string name,
                  std::string T::*) && -> operator_description_builder<T>;

  auto named(std::string name, bool T::*) && -> operator_description_builder<T>;

  auto
  named(std::string name, duration T::*) && -> operator_description_builder<T>;

  operator operator_description() const;
};

struct load_file_args {
  std::string path;
  bool follow;
  bool mmap;
  duration timeout;
};

auto compile_described_operator(std::string name, ast::invocation inv,
                                compile_ctx ctx)
  -> failure_or<ir::operator_ptr>;

class describe_operator_plugin : public virtual op_parser_plugin {
private:
  template <class Args>
  class arg {
  public:
    auto map(std::function<
             auto(located<duration>, diagnostic_handler&)->located<duration>>
               f) && -> arg<Args>;

    auto
    validate(std::function<
             auto(located<duration>, diagnostic_handler& dh)->failure_or<void>>
               f) && -> arg<Args>;
  };

public:
  virtual auto describe() const -> operator_description = 0;

  template <class T, class Args>
  static auto positional(std::string name, T Args::*) -> arg<Args>;

  template <class T, class Args>
  static auto named(std::string name, T Args::*) -> arg<Args>;

  auto compile(ast::invocation inv, compile_ctx ctx) const
    -> failure_or<ir::operator_ptr> final;
};

class described_operator final : public ir::operator_base {
public:
  described_operator() = default;

  auto make(const describe_operator_plugin* plugin, ast::invocation inv,
            compile_ctx ctx) {
  }

  auto substitute(substitute_ctx ctx, bool instantiate)
    -> failure_or<void> override {
  }

  auto instantiate(prepare_ctx ctx) && -> failure_or<ir::executable> override {
  }

  template <class Inspector>
  friend auto inspect(Inspector& f, described_operator& x) -> bool {
    if constexpr (Inspector::is_loading) {
      if (not f.apply(x.name_)) {
        return false;
      }
      auto plugin = plugins::find<describe_operator_plugin>(x.name_);
      TENZIR_ASSERT(plugin);
      description_ = plugin->describe();
    } else {
      return f.apply(x.name_);
    }
  }

private:
  const describe_operator_plugin* plugin_;
};

// TODO: Spawn actor from args. Should be inspectable.

class load_file_plugin final : public describe_operator_plugin {
public:
  auto name() const -> std::string override {
    return "fancy_load_file";
  }

  auto describe() const -> operator_description override {
    // load_file path:string, [follow=bool, mmap=bool, timeout=duration]
    return operator_description{
      positional("path", &load_file_args::path),
      named("follow", &load_file_args::follow),
      named("mmap", &load_file_args::mmap),
      named("timeout", &load_file_args::timeout)
        .validate(
          [](located<duration> x, diagnostic_handler& dh) -> failure_or<void> {
            if (x.inner <= duration::zero()) {
              diagnostic::error("duration must be strictly positive")
                .primary(x)
                .emit(dh);
              return failure::promise();
            }
            return {};
          }),
    };
  }
};

} // namespace tenzir
