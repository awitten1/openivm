#ifndef OPENIVM_PLAN_REWRITE_INTERNAL_HPP
#define OPENIVM_PLAN_REWRITE_INTERNAL_HPP

#include "duckdb.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

AggregateFunction BindAggregateByName(ClientContext &context, const string &name, const vector<LogicalType> &arg_types);
unique_ptr<BoundAggregateExpression> MakeAggregate(ClientContext &context, const string &name,
                                                   unique_ptr<Expression> child, const string &alias);
unique_ptr<BoundAggregateExpression> MakeCountStar(const string &alias);
LogicalOperator *FindProjectionAggregateInput(unique_ptr<LogicalOperator> &plan, bool allow_having_filter);
// Expose `binding` through `projection_path` (root first, extended bottom-up) under `alias`. With `reuse_existing`, a
// projection that already passes the binding through is reused. Returns whether any column was appended.
bool PropagateHiddenBindingThroughProjectionPath(vector<LogicalProjection *> &projection_path, ColumnBinding binding,
                                                 LogicalType type, const string &alias, bool reuse_existing = false);
void RewriteDerivedAggregates(ClientContext &context, unique_ptr<LogicalOperator> &plan, Optimizer &opt,
                              bool is_top = true);
void InjectSumNonNullCounts(ClientContext &context, unique_ptr<LogicalOperator> &plan);
void PropagateHiddenAggregateColumns(unique_ptr<LogicalOperator> &plan);
bool RewriteSafeSemiAntiDelimGets(ClientContext &context, unique_ptr<LogicalOperator> &plan);

} // namespace duckdb

#endif // OPENIVM_PLAN_REWRITE_INTERNAL_HPP
