/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.analytics.spi.FieldStorageInfo;
import org.opensearch.analytics.spi.ScalarFunctionAdapter;

import java.util.List;

/**
 * Adapter for the PPL {@code cluster_assign(message, reps_json, t, match, delims)} scalar UDF.
 * Rewrites the SQL plugin's {@code PPLBuiltinOperators.CLUSTER_ASSIGN} call (a custom Calcite UDF
 * named {@code "CLUSTER_ASSIGN"}) to {@link #LOCAL_CLUSTER_ASSIGN_OP}, which
 * {@link DataFusionFragmentConvertor#ADDITIONAL_SCALAR_SIGS} maps to the {@code cluster_assign}
 * Substrait extension (Rust UDF at
 * {@code sandbox/plugins/analytics-backend-datafusion/rust/src/udf/cluster_assign.rs}).
 *
 * <p>The {@code reps_json}, {@code t}, {@code match}, and {@code delims} operands MUST be literals —
 * the representative set is parsed once per query and the threshold / match mode / delimiter spec
 * are constants, so a column-valued operand is rejected at plan time with
 * {@code IllegalArgumentException} (same guarantee {@link RexExtractAdapter} makes for the regex
 * pattern / group). The {@code message} operand is a column.
 *
 * <p>Mirrors the {@link RexExtractAdapter} shape: one local target operator, literal-validation
 * steps, and a {@code rexBuilder.makeCall} preserving the original call's return type so the
 * enclosing project's row-type cache stays consistent.
 *
 * @opensearch.internal
 */
class ClusterAssignAdapter implements ScalarFunctionAdapter {

    /**
     * Locally-declared target operator for the rewrite. {@link SqlKind#OTHER_FUNCTION} so it
     * doesn't collide with any Calcite built-in. Operand-type checking is permissive
     * ({@code (CHARACTER, CHARACTER, NUMERIC, CHARACTER, CHARACTER)}) — the Rust UDF's
     * {@code coerce_types} does the real vetting. The declared return-type inference is a
     * placeholder; {@link #adapt} clones with {@code original.getType()}, so the adapted call's
     * reported type equals PPL's nullable INTEGER.
     */
    static final SqlOperator LOCAL_CLUSTER_ASSIGN_OP = new SqlFunction(
        "cluster_assign",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.INTEGER_NULLABLE,
        null,
        OperandTypes.family(
            SqlTypeFamily.CHARACTER,
            SqlTypeFamily.CHARACTER,
            SqlTypeFamily.NUMERIC,
            SqlTypeFamily.CHARACTER,
            SqlTypeFamily.CHARACTER),
        SqlFunctionCategory.USER_DEFINED_FUNCTION
    );

    @Override
    public RexNode adapt(RexCall original, List<FieldStorageInfo> fieldStorage, RelOptCluster cluster) {
        if (original.getOperands().size() != 5) {
            throw new IllegalArgumentException(
                "cluster_assign: expected 5 operands (message, reps_json, t, match, delims), got "
                    + original.getOperands().size());
        }
        validateStringLiteral(original.getOperands().get(1), "reps_json");
        validateNumericLiteral(original.getOperands().get(2), "t");
        validateStringLiteral(original.getOperands().get(3), "match");
        validateStringLiteral(original.getOperands().get(4), "delims");
        RexBuilder rexBuilder = cluster.getRexBuilder();
        // PPL lexes a bare decimal (e.g. `0.4`) as DECIMAL, but the substrait/YAML overload for the
        // threshold is fp64. Normalise `t` to a DOUBLE literal so isthmus binds the cluster_assign
        // fp64 impl on the DataFusion route (idempotent for an already-DOUBLE literal). Mirrors the
        // makeApproxLiteral(BigDecimal, DOUBLE) idiom in DateAddSubAdapter.
        RexLiteral tLit = (RexLiteral) original.getOperands().get(2);
        org.apache.calcite.rel.type.RelDataType fp64 =
            cluster.getTypeFactory().createSqlType(SqlTypeName.DOUBLE);
        RexNode tDouble =
            rexBuilder.makeApproxLiteral(tLit.getValueAs(java.math.BigDecimal.class), fp64);
        return rexBuilder.makeCall(
            original.getType(),
            LOCAL_CLUSTER_ASSIGN_OP,
            List.of(
                original.getOperands().get(0),
                original.getOperands().get(1),
                tDouble,
                original.getOperands().get(3),
                original.getOperands().get(4)));
    }

    /**
     * Reject a non-literal / non-string operand at plan time — the representative set is parsed
     * once per query and the match/delims specs are constants, so a column-valued operand would
     * either re-parse per row or silently degrade by treating the first row's value as a constant.
     */
    static void validateStringLiteral(RexNode operand, String name) {
        if (!(operand instanceof RexLiteral literal)) {
            throw new IllegalArgumentException("cluster_assign: '" + name + "' must be a string literal, got " + operand.getKind());
        }
        SqlTypeName typeName = literal.getType().getSqlTypeName();
        if (typeName != SqlTypeName.CHAR && typeName != SqlTypeName.VARCHAR) {
            throw new IllegalArgumentException("cluster_assign: '" + name + "' must be a string literal, got " + typeName);
        }
    }

    /** Reject a non-literal / non-numeric {@code t} at plan time — the threshold is a constant. */
    static void validateNumericLiteral(RexNode operand, String name) {
        if (!(operand instanceof RexLiteral literal)) {
            throw new IllegalArgumentException("cluster_assign: '" + name + "' must be a numeric literal, got " + operand.getKind());
        }
        if (!SqlTypeName.NUMERIC_TYPES.contains(literal.getType().getSqlTypeName())) {
            throw new IllegalArgumentException(
                "cluster_assign: '" + name + "' must be a numeric literal, got " + literal.getType().getSqlTypeName()
            );
        }
    }
}
