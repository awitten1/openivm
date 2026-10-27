#ifndef OPENIVM_DELTA_JOIN_HPP
#define OPENIVM_DELTA_JOIN_HPP

#include "delta/delta_helpers.hpp"
#include "delta/delta_operator.hpp"
#include "duckdb/optimizer/column_binding_replacer.hpp"
#include "duckdb/planner/column_binding_map.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"

namespace duckdb {

struct JoinLeafInfo {
	vector<size_t> path;
	LogicalGet *get;            // non-null for simple table scans
	LogicalOperator *node;      // always set; for non-GET leaves, rewrite the subtree
	bool is_right_of_left_join; // true if this leaf is on the nullable side of an outer join
};

LogicalGet *FindGetInSubtree(LogicalOperator *node);

unique_ptr<LogicalOperator> &GetNodeAtPath(unique_ptr<LogicalOperator> &root, const vector<size_t> &path);

void DemoteLeftJoins(LogicalOperator *node);

void UpdateParentProjectionMap(unique_ptr<LogicalOperator> &term, const JoinLeafInfo &leaf,
                               const ColumnBinding &mul_binding);

void AppendMultiplicityToAncestorProjectionMaps(unique_ptr<LogicalOperator> &term, const vector<size_t> &leaf_path,
                                                const ColumnBinding &mul_binding, const char *context_label,
                                                bool preserve_constant_sibling_child_outputs = false,
                                                idx_t fallback_mul_idx = DConstants::INVALID_INDEX);

// lhs * rhs, bound as DuckDB's multiplication operator.
unique_ptr<Expression> MultiplyIntegers(Binder &binder, unique_ptr<Expression> lhs, unique_ptr<Expression> rhs);

// The product of a term's leaf multiplicities, optionally times the Möbius sign (-1)^(k-1).
unique_ptr<Expression> BuildJoinMultiplicityProduct(Binder &binder, const LogicalType &mul_type,
                                                    const vector<ColumnBinding> &mul_bindings, bool mobius_sign);

// Project a join term to its outputs, minus `hidden` and the leaf multiplicities, plus the combined multiplicity.
unique_ptr<LogicalOperator> FinalizeJoinTerm(Binder &binder, unique_ptr<LogicalOperator> term,
                                             column_binding_set_t hidden, const vector<ColumnBinding> &mul_bindings,
                                             const LogicalType &mul_type, bool mobius_sign);

unique_ptr<LogicalOperator> AssembleJoinUnionAll(vector<unique_ptr<LogicalOperator>> &terms,
                                                 const vector<LogicalType> &types, Binder &binder);

ColumnBinding ReplaceJoinOutputBindings(const vector<ColumnBinding> &original_bindings,
                                        unique_ptr<LogicalOperator> &result, LogicalOperator &root);

} // namespace duckdb

#endif // OPENIVM_DELTA_JOIN_HPP
