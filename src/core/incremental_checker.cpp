#include "core/incremental_checker.hpp"

#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_unnest_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_any_join.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_dependent_join.hpp"
#include "duckdb/planner/operator/logical_distinct.hpp"
#include "duckdb/planner/operator/logical_positional_join.hpp"
#include "duckdb/planner/operator/logical_sample.hpp"
#include "duckdb/planner/operator/logical_window.hpp"
#include "duckdb/planner/expression/bound_window_expression.hpp"
#include "duckdb/planner/operator/logical_top_n.hpp"
#include "duckdb/planner/operator/logical_set_operation.hpp"
#include "duckdb/planner/operator/logical_unnest.hpp"

#include <functional>
#include <unordered_set>

namespace duckdb {

static const unordered_set<string> &GetSupportedAggregates() {
	static const unordered_set<string> kSet = {
	    "count_star", "count",    "sum",      "min",     "max",      "avg",     "list",    "stddev", "stddev_samp",
	    "stddev_pop", "variance", "var_samp", "var_pop", "bool_and", "bool_or", "arg_min", "arg_max"};
	return kSet;
}

static bool WindowColumn(const Expression &expr, string &name) {
	if (expr.type != ExpressionType::BOUND_COLUMN_REF) {
		return false;
	}
	auto &column = expr.Cast<BoundColumnRefExpression>();
	name = column.alias.empty() ? column.GetName() : column.alias;
	return !name.empty();
}

static bool SameWindowColumns(const vector<string> &left, const vector<string> &right) {
	if (left.size() != right.size()) {
		return false;
	}
	for (idx_t i = 0; i < left.size(); i++) {
		if (!StringUtil::CIEquals(left[i], right[i])) {
			return false;
		}
	}
	return true;
}

static bool HasSimpleOrderBy(const vector<BoundOrderByNode> &orders) {
	for (auto &order : orders) {
		if (order.expression->type != ExpressionType::BOUND_COLUMN_REF) {
			return false;
		}
		auto &column = order.expression->Cast<BoundColumnRefExpression>();
		if (column.alias.empty() && column.GetName().empty()) {
			return false;
		}
	}
	return true;
}

static bool AnyExpression(vector<unique_ptr<Expression>> &expressions,
                          const std::function<bool(const Expression &)> &predicate) {
	for (auto &expr : expressions) {
		bool found = false;
		ExpressionIterator::EnumerateExpression(expr, [&](Expression &child) { found = found || predicate(child); });
		if (found) {
			return true;
		}
	}
	return false;
}

static bool IsVolatileExpression(const Expression &expr) {
	if (expr.expression_class != ExpressionClass::BOUND_FUNCTION) {
		return false;
	}
	auto &name = expr.Cast<BoundFunctionExpression>().function.name;
	if (StringUtil::StartsWith(name, "__internal_compress") || StringUtil::StartsWith(name, "__internal_decompress") ||
	    name == "error") {
		return false;
	}
	return expr.Cast<BoundFunctionExpression>().function.GetStability() != FunctionStability::CONSISTENT;
}

static bool IsNonFoldableUnnestExpression(const Expression &expr) {
	if (expr.expression_class != ExpressionClass::BOUND_UNNEST) {
		return false;
	}
	auto &unnest = expr.Cast<BoundUnnestExpression>();
	return !unnest.child || !unnest.child->IsFoldable();
}

/// Expressions whose non-deterministic functions make the operator incompatible with IVM.
static vector<unique_ptr<Expression>> *VolatileCheckedExpressions(LogicalOperator &op) {
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_UNNEST:
	case LogicalOperatorType::LOGICAL_FILTER:
	case LogicalOperatorType::LOGICAL_PROJECTION:
	case LogicalOperatorType::LOGICAL_UNION:
	case LogicalOperatorType::LOGICAL_DISTINCT:
	case LogicalOperatorType::LOGICAL_WINDOW:
		return &op.expressions;
	case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY:
		return &op.Cast<LogicalAggregate>().groups;
	default:
		return nullptr;
	}
}

// Safety facts that propagate from a materialized CTE body regardless of how the outer query consumes it.
static bool PlanAnalysis::*const NON_LOCAL_FACTS[] = {
    &PlanAnalysis::found_volatile_expression,   &PlanAnalysis::found_non_foldable_unnest,
    &PlanAnalysis::found_unsupported_aggregate, &PlanAnalysis::found_unsupported_filtered_aggregate,
    &PlanAnalysis::found_unsupported_join_type, &PlanAnalysis::found_unsupported_order_by,
    &PlanAnalysis::found_unsupported_operator,  &PlanAnalysis::found_asof_join,
    &PlanAnalysis::found_positional_join,       &PlanAnalysis::found_sample};

// Query-shape facts a pass-through CTE consumer inherits from the body.
static bool PlanAnalysis::*const SHAPE_FACTS[] = {
    &PlanAnalysis::found_aggregation,    &PlanAnalysis::found_projection,     &PlanAnalysis::found_having,
    &PlanAnalysis::found_distinct,       &PlanAnalysis::found_union_distinct, &PlanAnalysis::found_minmax,
    &PlanAnalysis::found_list,           &PlanAnalysis::found_filtered_list,  &PlanAnalysis::found_left_join,
    &PlanAnalysis::found_full_outer,     &PlanAnalysis::found_semi_anti_join, &PlanAnalysis::found_join,
    &PlanAnalysis::found_delim_join,     &PlanAnalysis::found_single_join,    &PlanAnalysis::found_window,
    &PlanAnalysis::found_count_distinct, &PlanAnalysis::found_grouping_sets,  &PlanAnalysis::found_nested_aggregate};

void AnalyzePlanOperator(LogicalOperator &op, PlanAnalysis &result) {
	auto *node = &op;
	auto *volatile_checked = VolatileCheckedExpressions(op);
	if (volatile_checked && AnyExpression(*volatile_checked, IsVolatileExpression)) {
		result.incremental_compatible = false;
		result.found_volatile_expression = true;
	}
	switch (node->type) {
	// Infrastructure nodes — always compatible, no metadata
	case LogicalOperatorType::LOGICAL_CREATE_TABLE:
	case LogicalOperatorType::LOGICAL_INSERT:
	case LogicalOperatorType::LOGICAL_DUMMY_SCAN:
	case LogicalOperatorType::LOGICAL_GET:
	case LogicalOperatorType::LOGICAL_EXPRESSION_GET:
	case LogicalOperatorType::LOGICAL_CHUNK_GET:
	case LogicalOperatorType::LOGICAL_DELIM_GET:
	case LogicalOperatorType::LOGICAL_CTE_REF:
	case LogicalOperatorType::LOGICAL_ORDER_BY:
		break;

	case LogicalOperatorType::LOGICAL_UNNEST:
	case LogicalOperatorType::LOGICAL_MATERIALIZED_CTE:
		break;

	case LogicalOperatorType::LOGICAL_FILTER:
		// Detect HAVING: a FILTER above an AGGREGATE
		if (!node->children.empty() && node->children[0]->type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
			result.found_having = true;
		}
		break;

	case LogicalOperatorType::LOGICAL_PROJECTION:
		result.found_projection = true;
		if (AnyExpression(node->expressions, IsNonFoldableUnnestExpression)) {
			result.incremental_compatible = false;
			result.found_non_foldable_unnest = true;
		}
		break;

	case LogicalOperatorType::LOGICAL_UNION:
		if (!node->Cast<LogicalSetOperation>().setop_all) {
			result.found_distinct = true;
			result.found_union_distinct = true;
		}
		break;

	case LogicalOperatorType::LOGICAL_SAMPLE: {
		result.found_sample = true;
		auto &sample = node->Cast<LogicalSample>();
		if (!sample.sample_options || !sample.sample_options->seed.IsValid()) {
			result.incremental_compatible = false;
			result.found_volatile_expression = true;
		}
		break;
	}

	case LogicalOperatorType::LOGICAL_DISTINCT: {
		result.found_distinct = true;
		// DISTINCT columns become group-by keys after IVM rewrite
		auto &distinct_node = node->Cast<LogicalDistinct>();
		if (!distinct_node.distinct_targets.empty()) {
			for (auto &target : distinct_node.distinct_targets) {
				result.aggregate_columns.emplace_back(target->GetName());
			}
		} else {
			// Plain DISTINCT: all child output columns are keys
			for (idx_t i = 0; i < node->children[0]->types.size(); i++) {
				result.aggregate_columns.emplace_back("col" + to_string(i));
			}
		}
		break;
	}

	case LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
	case LogicalOperatorType::LOGICAL_ASOF_JOIN:
	case LogicalOperatorType::LOGICAL_ANY_JOIN:
	case LogicalOperatorType::LOGICAL_DEPENDENT_JOIN:
	case LogicalOperatorType::LOGICAL_DELIM_JOIN:
	case LogicalOperatorType::LOGICAL_JOIN:
	case LogicalOperatorType::LOGICAL_CROSS_PRODUCT: {
		result.found_join = true;
		if (node->type == LogicalOperatorType::LOGICAL_ASOF_JOIN) {
			result.found_asof_join = true;
		}
		if (node->type == LogicalOperatorType::LOGICAL_DEPENDENT_JOIN ||
		    node->type == LogicalOperatorType::LOGICAL_DELIM_JOIN) {
			result.found_delim_join = true;
		}
		auto *join = dynamic_cast<LogicalJoin *>(node);
		if (join) {
			if (join->join_type != JoinType::INNER && join->join_type != JoinType::LEFT &&
			    join->join_type != JoinType::RIGHT && join->join_type != JoinType::OUTER &&
			    join->join_type != JoinType::SEMI && join->join_type != JoinType::ANTI &&
			    join->join_type != JoinType::MARK && join->join_type != JoinType::SINGLE) {
				result.incremental_compatible = false;
				result.found_unsupported_join_type = true;
			}
			if (join->join_type == JoinType::SINGLE) {
				result.found_single_join = true;
			}
			if (join->join_type == JoinType::SEMI || join->join_type == JoinType::ANTI ||
			    join->join_type == JoinType::MARK) {
				result.found_semi_anti_join = true;
			}
			if (join->join_type == JoinType::LEFT || join->join_type == JoinType::RIGHT ||
			    join->join_type == JoinType::OUTER) {
				result.found_left_join = true;
			}
			if (join->join_type == JoinType::OUTER) {
				result.found_full_outer = true;
			}
		}
		break;
	}

	case LogicalOperatorType::LOGICAL_POSITIONAL_JOIN:
		result.found_join = true;
		result.found_positional_join = true;
		break;

	case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY: {
		result.found_aggregation = true;
		auto &agg = node->Cast<LogicalAggregate>();
		// GROUPING SETS / ROLLUP / CUBE produce multiple grouping_sets entries.
		// Routed to RECOMPUTE (full DELETE+INSERT with empty-delta skip) in the parser.
		if (agg.grouping_sets.size() > 1) {
			result.found_grouping_sets = true;
		}
		for (auto &expr : agg.expressions) {
			if (expr->expression_class == ExpressionClass::BOUND_AGGREGATE) {
				auto &bound_agg = expr->Cast<BoundAggregateExpression>();
				const auto &supported = GetSupportedAggregates();
				const bool scalar_subquery_first =
				    bound_agg.function.name == "first" && result.group_index != DConstants::INVALID_INDEX;
				if (supported.find(bound_agg.function.name) == supported.end() && !scalar_subquery_first) {
					result.incremental_compatible = false;
					result.found_unsupported_aggregate = true;
				}
				// COUNT(DISTINCT x) can't be summed from delta counts, but group-recompute
				// (delete affected groups + re-insert from original query) is correct.
				if (bound_agg.IsDistinct()) {
					result.found_count_distinct = true;
				}
				if (bound_agg.filter) {
					// LIST keeps NULL elements, so LIST(CASE WHEN p THEN x ELSE NULL END)
					// is not equivalent to LIST(x) FILTER (WHERE p). Keep the raw FILTER
					// and route the view to affected-group recompute using the original SQL.
					if (bound_agg.function.name == "list") {
						result.found_filtered_list = true;
					} else {
						// Other FILTER aggregates should have been normalized before the
						// checker runs. If one reaches here, use full refresh.
						result.incremental_compatible = false;
						result.found_unsupported_filtered_aggregate = true;
					}
				}
				if (bound_agg.function.name == "min" || bound_agg.function.name == "max" ||
				    bound_agg.function.name == "arg_min" || bound_agg.function.name == "arg_max") {
					result.found_minmax = true;
				}
				// LIST aggregates aren't element-wise summable — different deltas
				// produce lists of different lengths/contents. Mark the view so the
				// upsert compiler uses group-recompute (delete affected groups,
				// re-insert from the original query) rather than the failing
				// list_reduce/lambda path.
				if (bound_agg.function.name == "list") {
					result.found_list = true;
				}
				// Record aggregate function types only for the outermost aggregate in the plan.
				// Nested aggregates (CTE re-aggregate, subquery-join inner agg) would misalign
				// aggregate_types with the output column count and confuse CompileAggregateGroups.
				// Compatibility flags (found_minmax, found_list, found_count_distinct) are
				// recorded for all aggregates above so unsafe inner aggs still gate classification.
				if (result.group_index == DConstants::INVALID_INDEX) {
					result.aggregate_types.push_back(bound_agg.function.name);
				}
			}
		}
		// Record group count and group_index for the outermost (first-found) aggregate only.
		// In plans with nested aggregates the DFS visits the outer aggregate first; overwriting
		// with an inner aggregate's binding causes find_group_cols to miss the top projection.
		if (result.group_index == DConstants::INVALID_INDEX) {
			result.group_count = agg.groups.size();
			result.group_index = agg.group_index;
		} else {
			// A second aggregate node below the outermost one → nested aggregate pattern.
			// COUNT(*)/COUNT(x) in the outer aggregate counts inner-group rows, not source rows,
			// so the standard linear delta rule is incorrect. Route to group-recompute.
			result.found_nested_aggregate = true;
		}
		break;
	}

	case LogicalOperatorType::LOGICAL_WINDOW: {
		result.found_window = true;
		auto &window = node->Cast<LogicalWindow>();
		// Extract PARTITION BY column names from ALL window expressions. Also retain
		// a common simple PARTITION BY + ORDER BY key when every window agrees. The
		// latter lets DuckLake pair old/new row occurrences without assuming that the
		// key is unique; differing or computed window keys keep the existing
		// partition-recompute path.
		for (auto &expr : window.expressions) {
			if (expr->expression_class == ExpressionClass::BOUND_WINDOW) {
				auto &win_expr = expr->Cast<BoundWindowExpression>();
				vector<string> current_partitions;
				vector<string> current_orders;
				for (auto &part : win_expr.partitions) {
					string col_name;
					if (!WindowColumn(*part, col_name)) {
						result.window_row_key_compatible = false;
						col_name = part->GetName();
					}
					current_partitions.push_back(col_name);
					// Avoid duplicates
					bool found = false;
					for (auto &existing : result.window_partition_columns) {
						if (existing == col_name) {
							found = true;
							break;
						}
					}
					if (!found) {
						result.window_partition_columns.push_back(col_name);
					}
				}
				for (auto &order : win_expr.orders) {
					string col_name;
					if (!WindowColumn(*order.expression, col_name)) {
						result.window_row_key_compatible = false;
						break;
					}
					current_orders.push_back(col_name);
				}
				if (current_partitions.empty() || current_orders.empty()) {
					result.window_row_key_compatible = false;
				}
				if (result.window_row_key_compatible) {
					if (result.window_order_columns.empty()) {
						result.window_order_columns = current_orders;
					} else if (!SameWindowColumns(result.window_order_columns, current_orders) ||
					           !SameWindowColumns(result.window_partition_columns, current_partitions)) {
						result.window_row_key_compatible = false;
						result.window_order_columns.clear();
					}
				}
				if (!result.window_row_key_compatible) {
					result.window_order_columns.clear();
				}
			}
		}
		break;
	}

	case LogicalOperatorType::LOGICAL_TOP_N: {
		// Function/CASE/cast order expressions need a projection alias before they can
		// be emitted in the user-facing top-k view.
		auto &top_n = node->Cast<LogicalTopN>();
		if (!HasSimpleOrderBy(top_n.orders)) {
			result.incremental_compatible = false; // non-column ORDER BY: fall through to FULL_REFRESH
			result.found_unsupported_order_by = true;
		}
		break;
	}

	case LogicalOperatorType::LOGICAL_LIMIT: {
		// Ordered top-k is maintained over an unlimited backing table and the
		// ORDER BY/LIMIT is applied by the user-facing view. A standalone LIMIT
		// remains in the backing-table plan; maintaining only its current rows
		// cannot refill the boundary after a deletion, so it requires full refresh.
		if (node->children.empty() || node->children[0]->type != LogicalOperatorType::LOGICAL_ORDER_BY) {
			result.incremental_compatible = false;
			result.found_unsupported_operator = true;
		}
		break;
	}

	default:
		// Unsupported operator type
		result.incremental_compatible = false;
		result.found_unsupported_operator = true;
		break;
	}
}

static void MergeWindowAnalysis(PlanAnalysis &result, PlanAnalysis &body) {
	if (!body.found_window) {
		return;
	}
	if (!result.found_window) {
		result.window_partition_columns = std::move(body.window_partition_columns);
		result.window_order_columns = std::move(body.window_order_columns);
		result.window_row_key_compatible = body.window_row_key_compatible;
		return;
	}
	bool compatible = result.window_row_key_compatible && body.window_row_key_compatible &&
	                  SameWindowColumns(result.window_partition_columns, body.window_partition_columns) &&
	                  SameWindowColumns(result.window_order_columns, body.window_order_columns);
	for (auto &column : body.window_partition_columns) {
		if (std::find(result.window_partition_columns.begin(), result.window_partition_columns.end(), column) ==
		    result.window_partition_columns.end()) {
			result.window_partition_columns.push_back(std::move(column));
		}
	}
	result.window_row_key_compatible = compatible;
	if (!compatible) {
		result.window_order_columns.clear();
	}
}

void MergeMaterializedCteBodyAnalysis(PlanAnalysis &result, PlanAnalysis body) {
	const bool outer_passthrough = !result.found_aggregation && !result.found_distinct && !result.found_join;
	result.incremental_compatible = result.incremental_compatible && body.incremental_compatible;
	for (auto fact : NON_LOCAL_FACTS) {
		result.*fact = result.*fact || body.*fact;
	}
	if (!outer_passthrough) {
		return;
	}
	MergeWindowAnalysis(result, body);
	for (auto fact : SHAPE_FACTS) {
		result.*fact = result.*fact || body.*fact;
	}
	D_ASSERT(result.aggregate_columns.empty());
	result.aggregate_columns = std::move(body.aggregate_columns);
	result.aggregate_types = std::move(body.aggregate_types);
	result.group_count = body.group_count;
	result.group_index = body.group_index;
}

} // namespace duckdb
