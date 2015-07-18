#ifndef VAST_EXPRESSION_H
#define VAST_EXPRESSION_H

#include "vast/data.h"
#include "vast/key.h"
#include "vast/offset.h"
#include "vast/operator.h"
#include "vast/type.h"
#include "vast/expr/normalize.h"
#include "vast/expr/validator.h"
#include "vast/util/assert.h"
#include "vast/util/variant.h"

namespace vast {

class expression;

/// Extracts the event type.
struct event_extractor : util::totally_ordered<event_extractor>
{
  friend bool operator==(event_extractor const&, event_extractor const&)
  {
    return true;
  }

  friend bool operator<(event_extractor const&, event_extractor const&)
  {
    return false;
  }
};


/// Extracts the event timestamp.
struct time_extractor : util::totally_ordered<time_extractor>
{
  friend bool operator==(time_extractor const&, time_extractor const&)
  {
    return true;
  }

  friend bool operator<(time_extractor const&, time_extractor const&)
  {
    return false;
  }
};

/// Extracts a specific event type.
struct type_extractor : util::totally_ordered<type_extractor>
{
  type_extractor() = default;

  explicit type_extractor(vast::type t)
    : type{std::move(t)}
  {
  }

  vast::type type;

  friend bool operator==(type_extractor const& lhs, type_extractor const& rhs)
  {
    return lhs.type == rhs.type;
  }

  friend bool operator<(type_extractor const& lhs, type_extractor const& rhs)
  {
    return lhs.type < rhs.type;
  }
};

/// Extracts one or more values.
struct schema_extractor : util::totally_ordered<schema_extractor>
{
  schema_extractor() = default;

  explicit schema_extractor(vast::key k)
    : key{std::move(k)}
  {
  }

  vast::key key;

  friend bool operator==(schema_extractor const& lhs,
                         schema_extractor const& rhs)
  {
    return lhs.key == rhs.key;
  }

  friend bool operator<(schema_extractor const& lhs,
                        schema_extractor const& rhs)
  {
    return lhs.key < rhs.key;
  }
};

/// Extracts a singular value, the "instantiation" of a ::schema_extractor.
struct data_extractor : util::totally_ordered<data_extractor>
{
  data_extractor() = default;

  explicit data_extractor(vast::type t, vast::offset o)
    : type{std::move(t)},
      offset{std::move(o)}
  {
  }

  vast::type type;
  vast::offset offset;

  friend bool operator==(data_extractor const& lhs, data_extractor const& rhs)
  {
    return lhs.type == rhs.type && lhs.offset == rhs.offset;
  }

  friend bool operator<(data_extractor const& lhs, data_extractor const& rhs)
  {
    return std::tie(lhs.type, lhs.offset) < std::tie(rhs.type, rhs.offset);
  }
};

/// A predicate.
struct predicate : util::totally_ordered<predicate>
{
  predicate() = default;

  using operand = util::variant<
      event_extractor,
      time_extractor,
      type_extractor,
      schema_extractor,
      data_extractor,
      data
    >;

  predicate(operand l, relational_operator o, operand r)
    : lhs{std::move(l)},
      op{o},
      rhs{std::move(r)}
  {
  }

  operand lhs;
  relational_operator op;
  operand rhs;

  friend bool operator==(predicate const& lhs, predicate const& rhs)
  {
    return lhs.lhs == rhs.lhs && lhs.op == rhs.op && lhs.rhs == rhs.rhs;
  }

  friend bool operator<(predicate const& lhs, predicate const& rhs)
  {
    return
      std::tie(lhs.lhs, lhs.op, lhs.rhs) < std::tie(rhs.lhs, rhs.op, rhs.rhs);
  }
};

/// A sequence of AND expressions.
struct conjunction : std::vector<expression>
{
  using std::vector<expression>::vector;
};

/// A sequence of OR expressions.
struct disjunction : std::vector<expression>
{
  using std::vector<expression>::vector;
};

/// A NOT expression.
struct negation : std::vector<vast::expression>
{
  using std::vector<vast::expression>::vector;

  vast::expression const& expression() const;
  vast::expression& expression();
};

/// A query expression.
class expression : util::totally_ordered<expression>
{
  friend access;

public:
  using node = util::variant<
    none,
    conjunction,
    disjunction,
    negation,
    predicate
  >;

  /// Default-constructs empty an expression.
  expression(none = nil) {}

  /// Constructs an expression.
  /// @param x The node to construct an expression from.
  template <
    typename T,
    typename U = std::decay_t<T>,
    typename = std::enable_if_t<
         std::is_same<U, none>::value
      || std::is_same<U, conjunction>::value
      || std::is_same<U, disjunction>::value
      || std::is_same<U, negation>::value
      || std::is_same<U, predicate>::value
    >
  >
  expression(T&& x)
    : node_{std::forward<T>(x)}
  {
  }

  friend bool operator==(expression const& lhs, expression const& rhs);
  friend bool operator<(expression const& lhs, expression const& rhs);

  friend node& expose(expression& d);
  friend node const& expose(expression const& d);

private:
  node node_;
};

namespace detail {

// Helper function constructing an expression from a string in order to save
// compile times.
trial<expression> to_expression(std::string const& str);

} // namespace detail

} // namespace vast

#endif
