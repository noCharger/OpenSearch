/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.rel;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;

import java.util.List;

/**
 * Plain Calcite SingleRel representing an unnest operation for Substrait emission.
 * Created by {@link OpenSearchUnnest#stripAnnotations} and recognized by
 * {@code DataFusionFragmentConvertor} to emit an ExtensionSingleRel.
 *
 * <p>This node carries the unnest parameters (array column index, element type,
 * ordinality flag) in a form the convertor can serialize to the ExtensionRel detail.
 * The convertor does NOT need to reconstruct the original Correlate+Uncollect shape.
 *
 * <p>Row type: input row type + appended element field (same as OpenSearchUnnest).
 *
 * @opensearch.internal
 */
public class CalciteUnnestRel extends SingleRel {

    private final int unnestColumnIndex;
    private final RelDataTypeField elementField;
    private final boolean withOrdinality;

    /**
     * @param cluster           Calcite cluster
     * @param traitSet          trait set (inherited from the marked OpenSearchUnnest)
     * @param input             the stripped child
     * @param unnestColumnIndex index of the array column in input's rowType
     * @param elementField      the appended element field (last column of output)
     * @param withOrdinality    whether to emit an ordinal column
     */
    public CalciteUnnestRel(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        RelNode input,
        int unnestColumnIndex,
        RelDataTypeField elementField,
        boolean withOrdinality
    ) {
        super(cluster, traitSet, input);
        this.unnestColumnIndex = unnestColumnIndex;
        this.elementField = elementField;
        this.withOrdinality = withOrdinality;
        this.rowType = deriveRowType();
    }

    @Override
    protected RelDataType deriveRowType() {
        if (input == null) {
            // Called from super constructor before input is set
            return null;
        }
        RelDataType inputRowType = input.getRowType();
        var builder = getCluster().getTypeFactory().builder();
        for (RelDataTypeField f : inputRowType.getFieldList()) {
            builder.add(f.getName(), f.getType());
        }
        builder.add(elementField.getName(), elementField.getType());
        return builder.build();
    }

    public int getUnnestColumnIndex() {
        return unnestColumnIndex;
    }

    public RelDataTypeField getElementField() {
        return elementField;
    }

    public boolean isWithOrdinality() {
        return withOrdinality;
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new CalciteUnnestRel(
            getCluster(),
            traitSet,
            sole(inputs),
            unnestColumnIndex,
            elementField,
            withOrdinality
        );
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw)
            .item("unnestColumnIndex", unnestColumnIndex)
            .item("elementField", elementField.getName() + ":" + elementField.getType())
            .item("withOrdinality", withOrdinality);
    }
}
