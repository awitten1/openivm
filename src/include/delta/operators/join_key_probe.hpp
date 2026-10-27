#ifndef OPENIVM_DELTA_JOIN_KEY_PROBE_HPP
#define OPENIVM_DELTA_JOIN_KEY_PROBE_HPP

#include "core/openivm_debug.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/planner/column_binding_map.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"

namespace duckdb {

struct DeltaJoinKeyProbe {
	size_t other_leaf;
	string delta_column;
	string other_column;
};

inline bool TryGetDeltaJoinColumnRef(Expression &expr, ColumnBinding &binding) {
	if (expr.GetExpressionClass() == ExpressionClass::BOUND_CAST) {
		auto &cast = expr.Cast<BoundCastExpression>();
		return TryGetDeltaJoinColumnRef(*cast.child, binding);
	}
	if (expr.GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF) {
		return false;
	}
	auto &col = expr.Cast<BoundColumnRefExpression>();
	binding = col.binding;
	return true;
}

template <class ColumnRef>
void CollectDeltaJoinKeyProbes(LogicalOperator *node, const column_binding_map_t<ColumnRef> &column_refs,
                               vector<vector<DeltaJoinKeyProbe>> &probes, const char *debug_label = nullptr) {
	if (!node) {
		return;
	}
	if (node->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
		auto *join = dynamic_cast<LogicalComparisonJoin *>(node);
		if (join && join->join_type == JoinType::INNER) {
			for (auto &cond : join->conditions) {
				if (cond.comparison != ExpressionType::COMPARE_EQUAL) {
					continue;
				}
				ColumnBinding left_binding, right_binding;
				if (!TryGetDeltaJoinColumnRef(*cond.left, left_binding) ||
				    !TryGetDeltaJoinColumnRef(*cond.right, right_binding)) {
					continue;
				}
				if (debug_label) {
					OPENIVM_DEBUG_PRINT("[%s] Join key bindings: %s = %s\n", debug_label,
					                    left_binding.ToString().c_str(), right_binding.ToString().c_str());
				}
				auto left_entry = column_refs.find(left_binding);
				auto right_entry = column_refs.find(right_binding);
				if (left_entry == column_refs.end() || right_entry == column_refs.end()) {
					continue;
				}
				auto &left = left_entry->second;
				auto &right = right_entry->second;
				if (left.leaf_index == right.leaf_index) {
					continue;
				}
				probes[left.leaf_index].push_back({right.leaf_index, left.column_name, right.column_name});
				probes[right.leaf_index].push_back({left.leaf_index, right.column_name, left.column_name});
			}
		}
	}
	for (auto &child : node->children) {
		CollectDeltaJoinKeyProbes(child.get(), column_refs, probes, debug_label);
	}
}

} // namespace duckdb

#endif // OPENIVM_DELTA_JOIN_KEY_PROBE_HPP
