#ifndef FRAMEWORK_UNIT_H
#define FRAMEWORK_UNIT_H

#include <cassert>
#include <map>
#include <vector>
#include <string>
#include <iostream>

#include "framework/color.h"
#include "framework/configuration.h"

namespace unit {

struct require_error : std::logic_error
{
  using logic_error::logic_error;
};

/// A sequence of checks.
class test
{
public:
  test(std::string name);

  virtual ~test() = default;

  virtual void __run() = 0;

  void __pass(std::string msg);
  void __fail(std::string msg, bool expected);

  size_t __expected_failures() const;
  std::vector<std::pair<bool, std::string>> const& __trace() const;
  std::string const& __name() const;

private:
  size_t expected_failures_ = 0;
  std::vector<std::pair<bool, std::string>> trace_;
  std::string name_;
};

/// Drives unit test execution.
class engine
{
  engine() = default;

public:
  /// Adds a suite to the engine.
  /// @param name The name of the suite.
  /// @param t The test to register.
  /// @throws `std::logic_error` if a test with *t*'s name exists already.
  static void add(char const* name, std::unique_ptr<test> t);

  /// Invokes tests in all suites.
  /// @param cfg The configuration to run with.
  /// @returns `true` iff all tests succeeded.
  static bool run(configuration const& cfg);

  /// Retrieves the file of the last check.
  /// @returns The source file where the last successful check came from.
  static char const* last_check_file();

  /// Sets the file of the last check.
  /// @param file The file of the last successful check.
  static void last_check_file(char const* file);

  /// Retrieves the line number of the last check.
  /// @returns The line where the last successful check came from.
  static size_t last_check_line();

  /// Sets the file of the last check.
  /// @param line The line of the last successful check.
  static void last_check_line(size_t line);

  /// Retrieves the currently executing test.
  /// @returns A pointer to the currently executing test.
  static test* current_test();

private:
  static engine& instance();

  test* current_ = nullptr;
  std::map<std::string, std::vector<std::unique_ptr<test>>> suites_;
};

namespace detail {

extern char const* suite;

struct namer
{
  namer(char const* name)
  {
    suite = name;
  }

  ~namer()
  {
    suite = nullptr;
  }
};

template <typename T>
struct adder
{
  adder()
  {
    engine::add(suite, std::make_unique<T>());
  }
};

template <typename T>
struct showable_base {};

template <typename T>
std::ostream& operator<<(std::ostream& out, showable_base<T> const&)
{
  out << color::blue << "<unprintable>" << color::reset;
  return out;
}

// See GCC note below.
#ifdef __GNUC__
struct stream
{
  template <typename S, typename U>
  static auto test(S* o, U const* x) -> decltype(*o << *x, std::true_type());

  template <typename, typename>
  static auto test(...) -> std::false_type;
};

template <typename S, typename U>
struct streamable : decltype(stream::test<S, U>(0, 0)) {};
#endif

template <typename T>
class showable : public showable_base<T>
{
public:
  explicit showable(T const& x)
    : x_(x)
  {
  }

  // GCC 4.9 appears to choke on expression SFINAE here and attempts to
  // instantiate the function even when the decltype expression fails. 
#ifndef __GNUC__
  template <typename U = T>
  friend auto operator<<(std::ostream& out, showable const& p)
    -> decltype(out << std::declval<U const&>())
  {
    return out << p.x_;
  }
#else
  template <
    typename U = T,
    typename = std::enable_if_t<streamable<std::ostream, U>::value>
  >
  friend std::ostream& operator<<(std::ostream& out, showable const& p)
  {
    return out << p.x_;
  }
#endif

private:
  T const& x_;
};

template <typename T>
showable<T> show(T const &x)
{
  return showable<T>{x};
}

// Constructs spacing given a line number.
inline char const* fill(int line)
{
  if (line < 10)
    return "    ";
  else if (line < 100)
    return "   ";
  else if (line < 1000)
    return "  ";
  else
    return " ";
}

template <typename T>
struct lhs
{
public:
  lhs(test* parent, char const *file, int line, char const *expr,
      bool should_fail, T const& x)
    : test_(parent),
      filename_(file),
      line_(line),
      expr_(expr),
      should_fail_(should_fail),
      x_(x)
  {
    assert(test_ != nullptr);
  }

  ~lhs()
  {
    if (evaluated_)
      return;

    if (eval(0))
      pass();
    else
      fail_unary();
  }

  template <typename U>
  using elevated = std::conditional_t<std::is_convertible<U, T>::value, T, U>;

  explicit operator bool()
  {
    evaluated_ = true;
    return !! x_ ? pass() : fail_unary();
  }

  template <typename U>
  bool operator==(U const& u)
  {
    evaluated_ = true;
    return x_ == static_cast<elevated<U>>(u) ? pass() : fail(u);
  }

  template <typename U>
  bool operator!=(U const& u)
  {
    evaluated_ = true;
    return x_ != static_cast<elevated<U>>(u) ? pass() : fail(u);
  }

  template <typename U>
  bool operator<(U const& u)
  {
    evaluated_ = true;
    return x_ < static_cast<elevated<U>>(u) ? pass() : fail(u);
  }

  template <typename U>
  bool operator<=(U const& u)
  {
    evaluated_ = true;
    return x_ <= static_cast<elevated<U>>(u) ? pass() : fail(u);
  }

  template <typename U>
  bool operator>(U const& u)
  {
    evaluated_ = true;
    return x_ > static_cast<elevated<U>>(u) ? pass() : fail(u);
  }

  template <typename U>
  bool operator>=(U const& u)
  {
    evaluated_ = true;
    return x_ >= static_cast<elevated<U>>(u) ? pass() : fail(u);
  }

private:
  template<typename V = T>
  auto eval(int) -> decltype(! std::declval<V>())
  {
    return !! x_;
  }

  bool eval(long)
  {
    return true;
  }

  bool pass()
  {
    passed_ = true;

    std::stringstream ss;
    ss
      << color::green << "** "
      << color::blue << filename_ << color::yellow << ":"
      << color::blue << line_ << fill(line_) << color::reset << expr_;

    test_->__pass(ss.str());

    return true;
  }

  bool fail_unary()
  {
    std::stringstream ss;
    ss
      << color::red << "!! "
      << color::blue << filename_ << color::yellow << ":"
      << color::blue << line_ << fill(line_) << color::reset << expr_;

    test_->__fail(ss.str(), should_fail_);

    return false;
  }

  template <typename U>
  bool fail(U const& u)
  {
    std::stringstream ss;
    ss
      << color::red << "!! "
      << color::blue << filename_ << color::yellow << ":"
      << color::blue << line_ << fill(line_) << color::reset << expr_
      << color::magenta << " ("
      << color::red << show(x_) << color::magenta << " !! "
      << color::red << show(u) << color::magenta << ')' << color::reset;

    test_->__fail(ss.str(), should_fail_);

    return false;
  }

  bool evaluated_ = false;
  bool passed_ = false;
  test* test_;
  char const *filename_;
  int line_;
  char const *expr_;
  bool should_fail_;
  T const& x_;
};

struct expr
{
public:
  expr(test* parent, char const *filename, int lineno, bool should_fail,
       char const *expr)
    : test_{parent},
      filename_{filename},
      line_{lineno},
      should_fail_{should_fail},
      expr_{expr}
  {
  }

  template <typename T>
  lhs<T> operator->*(T const& x)
  {
    return {test_, filename_, line_, expr_, should_fail_, x};
  }

private:
  test* test_;
  char const* filename_;
  int line_;
  bool should_fail_;
  char const* expr_;
};

} // namespace detail
} // namespace unit

#define UNIT_CONCAT(lhs, rhs) lhs ## rhs
#define UNIT_PASTE(lhs, rhs) UNIT_CONCAT(lhs, rhs)
#define UNIT_UNIQUE(name) UNIT_PASTE(name, __LINE__)

#define CHECK(...)                                                          \
  do                                                                        \
  {                                                                         \
    (void)(::unit::detail::expr{::unit::engine::current_test(),             \
                                __FILE__, __LINE__, false,                  \
                                #__VA_ARGS__} ->* __VA_ARGS__);             \
                                                                            \
    ::unit::engine::last_check_file(__FILE__);                              \
    ::unit::engine::last_check_line(__LINE__);                              \
  }                                                                         \
  while (false)

#define FAIL(...)                                                           \
  do                                                                        \
  {                                                                         \
    (void)(::unit::detail::expr{::unit::engine::current_test(),             \
                                __FILE__, __LINE__, true,                   \
                                #__VA_ARGS__} ->* __VA_ARGS__);             \
                                                                            \
    ::unit::engine::last_check_file(__FILE__);                              \
    ::unit::engine::last_check_line(__LINE__);                              \
  }                                                                         \
  while (false)

#define REQUIRE(...)                                                        \
  do                                                                        \
  {                                                                         \
    auto UNIT_UNIQUE(__result) =                                            \
      ::unit::detail::expr{::unit::engine::current_test(),                  \
                           __FILE__, __LINE__, false, #__VA_ARGS__}         \
      ->* __VA_ARGS__;                                                      \
                                                                            \
    if (! UNIT_UNIQUE(__result))                                            \
      throw ::unit::require_error{#__VA_ARGS__};                            \
                                                                            \
    ::unit::engine::last_check_file(__FILE__);                              \
    ::unit::engine::last_check_line(__LINE__);                              \
  }                                                                         \
  while (false)


#define SUITE(name)                                                         \
  namespace { ::unit::detail::namer UNIT_UNIQUE(namer){name}; }

#define TEST(name)                                                          \
  namespace {                                                               \
                                                                            \
  struct UNIT_UNIQUE(test) : public ::unit::test                            \
  {                                                                         \
    UNIT_UNIQUE(test)() : test{name} { }                                    \
    void __run() final;                                                     \
  };                                                                        \
                                                                            \
  ::unit::detail::adder<UNIT_UNIQUE(test)> UNIT_UNIQUE(a){};                \
                                                                            \
  } /* namespace <anonymous> */                                             \
                                                                            \
  void UNIT_UNIQUE(test)::__run()

#endif
