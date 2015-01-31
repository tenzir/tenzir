#include "vast/expr/predicatizer.h"
#include "vast/expression.h"

namespace vast {
namespace expr {

std::vector<predicate> predicatizer::operator()(none) const
{
  return {};
}

std::vector<predicate> predicatizer::operator()(conjunction const& con) const
{
  std::vector<predicate> preds;
  for (auto& op : con)
  {
    auto ps = visit(*this, op);
    std::move(ps.begin(), ps.end(), std::back_inserter(preds));
  }
  return preds;
}

std::vector<predicate> predicatizer::operator()(disjunction const& dis) const
{
  std::vector<predicate> preds;
  for (auto& op : dis)
  {
    auto ps = visit(*this, op);
    std::move(ps.begin(), ps.end(), std::back_inserter(preds));
  }
  return preds;
}

std::vector<predicate> predicatizer::operator()(negation const& n) const
{
  return visit(*this, n[0]);
}

std::vector<predicate> predicatizer::operator()(predicate const& pred) const
{
  return {pred};
}

} // namespace expr
} // namespace vast
