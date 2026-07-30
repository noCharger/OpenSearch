/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.rules;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Correlate;
import org.apache.calcite.rel.core.Uncollect;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.util.ImmutableBitSet;
import org.opensearch.analytics.planner.PlannerContext;
import org.opensearch.analytics.planner.RelNodeUtils;
import org.opensearch.analytics.planner.rel.OpenSearchRelNode;
import org.opensearch.analytics.planner.rel.OpenSearchUnnest;
import org.opensearch.analytics.spi.EngineCapability;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Converts the Calcite {@code LogicalCorrelate(LEFT, requiredColumns={i}) + Uncollect}
 * idiom (PPL mvexpand/multikv lowering) to {@link OpenSearchUnnest}.
 *
 * <p>Pattern:
 * <pre>
 *   Correlate(inner, requiredColumns={i})
 *     LEFT  = marked OpenSearchRelNode with array column at index i
 *     RIGHT = Uncollect(Project($cor0.array_col), Values(1row))
 * </pre>
 *
 * <p>Output row type = left cols ++ appended element col (element LAST). Parent
 * Project refs the element by index (last field of Correlate rowType).
 *
 * <p>Viable backends = intersection of left child's backends with those declaring
 * {@link EngineCapability#UNNEST}. Throws if empty.
 *
 * @opensearch.internal
 */
public class OpenSearchUnnestRule extends RelOptRule {

    private final PlannerContext context;

    public OpenSearchUnnestRule(PlannerContext context) {
        // Match Correlate with Uncollect as the right child
        super(
            operand(Correlate.class, operand(RelNode.class, any()), operand(Uncollect.class, any())),
            "OpenSearchUnnestRule"
        );
        this.context = context;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        Correlate correlate = call.rel(0);
        RelNode left = call.rel(1);
        Uncollect uncollect = call.rel(2);

        // Skip if already transformed
        if (left instanceof OpenSearchUnnest) {
            return;
        }

        // Unwrap HepRelVertex if needed
        RelNode leftUnwrapped = RelNodeUtils.unwrapHep(left);

        // Validate left child is marked
        if (!(leftUnwrapped instanceof OpenSearchRelNode osLeft)) {
            throw new IllegalStateException(
                "Unnest rule encountered unmarked child [" + leftUnwrapped.getClass().getSimpleName() + "]"
            );
        }

        // Extract unnest column index from requiredColumns
        ImmutableBitSet requiredColumns = correlate.getRequiredColumns();
        if (requiredColumns.cardinality() != 1) {
            throw new IllegalStateException(
                "Unnest rule expects exactly one required column, got " + requiredColumns.cardinality()
            );
        }
        int unnestColumnIndex = requiredColumns.nextSetBit(0);

        // Element field is the last field of the Correlate's rowType
        List<RelDataTypeField> correlateFields = correlate.getRowType().getFieldList();
        RelDataTypeField elementField = correlateFields.get(correlateFields.size() - 1);

        // Compute viable backends: intersect left's backends with UNNEST-capable backends
        List<String> leftBackends = osLeft.getViableBackends();
        List<String> unnestCapable = context.getCapabilityRegistry().operatorBackends(EngineCapability.UNNEST);

        Set<String> intersection = new LinkedHashSet<>(leftBackends);
        intersection.retainAll(unnestCapable);
        List<String> viableBackends = new ArrayList<>(intersection);

        if (viableBackends.isEmpty()) {
            throw new IllegalStateException(
                "No backend supports UNNEST capability among " + leftBackends
            );
        }

        call.transformTo(
            new OpenSearchUnnest(
                correlate.getCluster(),
                leftUnwrapped.getTraitSet(),
                leftUnwrapped,
                unnestColumnIndex,
                elementField,
                false, // withOrdinality = false for v1
                viableBackends
            )
        );
    }
}
