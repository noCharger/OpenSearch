/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.rel;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.opensearch.analytics.planner.RelNodeUtils;
import org.opensearch.analytics.spi.FieldStorageInfo;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;

/**
 * OpenSearch-marked unnest (row expansion) operator. Wraps the result of matching
 * a {@code LogicalCorrelate(LEFT, requiredColumns={i})} + {@code Uncollect} idiom
 * that PPL {@code mvexpand} / {@code multikv} lower to.
 *
 * <p>Row type: left child columns ++ appended element column (element LAST).
 * This matches the Calcite Correlate output shape so parent Project refs stay valid.
 *
 * <p>{@link #stripAnnotations} returns a {@link CalciteUnnestRel} carrying the
 * unnest parameters for Substrait emission — no Correlate/Uncollect reconstruction.
 *
 * @opensearch.internal
 */
public class OpenSearchUnnest extends SingleRel implements OpenSearchRelNode {

    private final int unnestColumnIndex;
    private final RelDataTypeField elementField;
    private final boolean withOrdinality;
    private final List<String> viableBackends;

    /**
     * @param cluster         Calcite cluster
     * @param traitSet        trait set (inherits from Correlate)
     * @param input           the left child of the original Correlate (already marked)
     * @param unnestColumnIndex index of the array column in input's rowType to unnest
     * @param elementField    the appended element field (last column of output)
     * @param withOrdinality  whether to emit an ordinal column (always false for v1)
     * @param viableBackends  backends declaring UNNEST capability
     */
    public OpenSearchUnnest(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        RelNode input,
        int unnestColumnIndex,
        RelDataTypeField elementField,
        boolean withOrdinality,
        List<String> viableBackends
    ) {
        super(cluster, traitSet, input);
        this.unnestColumnIndex = unnestColumnIndex;
        this.elementField = elementField;
        this.withOrdinality = withOrdinality;
        this.viableBackends = List.copyOf(viableBackends);
        // rowType = input rowType + appended element field
        this.rowType = computeRowType();
    }

    private RelDataType computeRowType() {
        RelDataType inputRowType = getInput().getRowType();
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
    public List<String> getViableBackends() {
        return viableBackends;
    }

    @Override
    public List<FieldStorageInfo> getOutputFieldStorage() {
        RelNode input = RelNodeUtils.unwrapHep(getInput());
        if (!(input instanceof OpenSearchRelNode osChild)) {
            throw new IllegalStateException("Unnest child is not OpenSearchRelNode: " + input.getClass().getSimpleName());
        }
        List<FieldStorageInfo> inputStorage = osChild.getOutputFieldStorage();
        List<FieldStorageInfo> result = new ArrayList<>(inputStorage.size() + 1);
        result.addAll(inputStorage);

        // Element column depends on the array column being unnested
        LinkedHashSet<String> deps = new LinkedHashSet<>();
        if (unnestColumnIndex < inputStorage.size()) {
            FieldStorageInfo arrayCol = inputStorage.get(unnestColumnIndex);
            if (arrayCol.isDerived()) {
                deps.addAll(arrayCol.getDependsOnPhysicalCols());
            } else {
                deps.add(arrayCol.getFieldName());
            }
        }
        result.add(FieldStorageInfo.derivedColumn(elementField.getName(), elementField.getType().getSqlTypeName(), deps));
        return result;
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new OpenSearchUnnest(
            getCluster(),
            traitSet,
            sole(inputs),
            unnestColumnIndex,
            elementField,
            withOrdinality,
            viableBackends
        );
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        // Unnest is a row-expansion — compute cost proportional to input rows * avg array length.
        // For simplicity, use tiny cost (it's a simple transformation).
        return planner.getCostFactory().makeTinyCost();
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw)
            .item("unnestColumnIndex", unnestColumnIndex)
            .item("elementField", elementField.getName())
            .item("withOrdinality", withOrdinality)
            .item("viableBackends", viableBackends);
    }

    @Override
    public RelNode copyResolved(String backend, List<RelNode> children, List<OperatorAnnotation> resolvedAnnotations) {
        return new OpenSearchUnnest(
            getCluster(),
            getTraitSet(),
            children.getFirst(),
            unnestColumnIndex,
            elementField,
            withOrdinality,
            List.of(backend)
        );
    }

    @Override
    public RelNode stripAnnotations(List<RelNode> strippedChildren) {
        return new CalciteUnnestRel(
            getCluster(),
            getTraitSet(),
            strippedChildren.getFirst(),
            unnestColumnIndex,
            elementField,
            withOrdinality
        );
    }
}
