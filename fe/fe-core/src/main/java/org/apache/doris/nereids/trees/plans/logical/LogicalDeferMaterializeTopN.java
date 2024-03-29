// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.nereids.trees.plans.logical;

import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.properties.ExprFdItem;
import org.apache.doris.nereids.properties.FdFactory;
import org.apache.doris.nereids.properties.FdItem;
import org.apache.doris.nereids.properties.FunctionalDependencies;
import org.apache.doris.nereids.properties.FunctionalDependencies.Builder;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.properties.OrderKey;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.algebra.TopN;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.Utils;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

/**
 * use for defer materialize top n
 */
public class LogicalDeferMaterializeTopN<CHILD_TYPE extends Plan> extends LogicalUnary<CHILD_TYPE>
        implements TopN {

    private final LogicalTopN<? extends Plan> logicalTopN;

    ///////////////////////////////////////////////////////////////////////////
    // Members for defer materialize for top-n opt.
    ///////////////////////////////////////////////////////////////////////////
    private final Set<ExprId> deferMaterializeSlotIds;
    private final SlotReference columnIdSlot;

    public LogicalDeferMaterializeTopN(LogicalTopN<CHILD_TYPE> logicalTopN,
            Set<ExprId> deferMaterializeSlotIds, SlotReference columnIdSlot) {
        super(PlanType.LOGICAL_TOP_N, logicalTopN.getGroupExpression(),
                Optional.of(logicalTopN.getLogicalProperties()), logicalTopN.child());
        this.logicalTopN = logicalTopN;
        this.deferMaterializeSlotIds = deferMaterializeSlotIds;
        this.columnIdSlot = columnIdSlot;
    }

    public LogicalDeferMaterializeTopN(LogicalTopN<? extends Plan> logicalTopN,
            Set<ExprId> deferMaterializeSlotIds, SlotReference columnIdSlot,
            Optional<GroupExpression> groupExpression, Optional<LogicalProperties> logicalProperties,
            CHILD_TYPE child) {
        super(PlanType.LOGICAL_TOP_N, groupExpression, logicalProperties, child);
        this.logicalTopN = logicalTopN;
        this.deferMaterializeSlotIds = deferMaterializeSlotIds;
        this.columnIdSlot = columnIdSlot;
    }

    public LogicalTopN<? extends Plan> getLogicalTopN() {
        return logicalTopN;
    }

    public Set<ExprId> getDeferMaterializeSlotIds() {
        return deferMaterializeSlotIds;
    }

    public SlotReference getColumnIdSlot() {
        return columnIdSlot;
    }

    @Override
    public List<OrderKey> getOrderKeys() {
        return logicalTopN.getOrderKeys();
    }

    @Override
    public long getOffset() {
        return logicalTopN.getOffset();
    }

    @Override
    public long getLimit() {
        return logicalTopN.getLimit();
    }

    @Override
    public List<? extends Expression> getExpressions() {
        return ImmutableList.<Expression>builder()
                .addAll(logicalTopN.getExpressions())
                .add(columnIdSlot).build();
    }

    @Override
    public List<Slot> computeOutput() {
        return logicalTopN.getOutput().stream()
                .filter(s -> !(s.getExprId().equals(columnIdSlot.getExprId())))
                .collect(ImmutableList.toImmutableList());
    }

    @Override
    public FunctionalDependencies computeFuncDeps(Supplier<List<Slot>> outputSupplier) {
        FunctionalDependencies fd = child(0).getLogicalProperties().getFunctionalDependencies();
        if (getLimit() == 1) {
            Builder builder = new Builder();
            List<Slot> output = outputSupplier.get();
            output.forEach(builder::addUniformSlot);
            output.forEach(builder::addUniqueSlot);
            ImmutableSet<FdItem> fdItems = computeFdItems(outputSupplier);
            builder.addFdItems(fdItems);
            fd = builder.build();
        }
        return fd;
    }

    @Override
    public ImmutableSet<FdItem> computeFdItems(Supplier<List<Slot>> outputSupplier) {
        ImmutableSet<FdItem> fdItems = child(0).getLogicalProperties().getFunctionalDependencies().getFdItems();
        if (getLimit() == 1) {
            ImmutableSet.Builder<FdItem> builder = ImmutableSet.builder();
            List<Slot> output = outputSupplier.get();
            ImmutableSet<SlotReference> slotSet = output.stream()
                    .filter(SlotReference.class::isInstance)
                    .map(SlotReference.class::cast)
                    .collect(ImmutableSet.toImmutableSet());
            ExprFdItem fdItem = FdFactory.INSTANCE.createExprFdItem(slotSet, true, slotSet);
            builder.add(fdItem);
            fdItems = builder.build();
        }
        return fdItems;
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitLogicalDeferMaterializeTopN(this, context);
    }

    @Override
    public Plan withGroupExpression(Optional<GroupExpression> groupExpression) {
        return new LogicalDeferMaterializeTopN<>(logicalTopN, deferMaterializeSlotIds, columnIdSlot,
                groupExpression, Optional.of(getLogicalProperties()), child());
    }

    @Override
    public Plan withGroupExprLogicalPropChildren(Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties, List<Plan> children) {
        Preconditions.checkArgument(children.size() == 1,
                "LogicalDeferMaterializeTopN should have 1 child, but input is %s", children.size());
        return new LogicalDeferMaterializeTopN<>(logicalTopN.withChildren(ImmutableList.of(children.get(0))),
                deferMaterializeSlotIds, columnIdSlot, groupExpression, logicalProperties, children.get(0));
    }

    @Override
    public Plan withChildren(List<Plan> children) {
        Preconditions.checkArgument(children.size() == 1,
                "LogicalDeferMaterializeTopN should have 1 child, but input is %s", children.size());
        return new LogicalDeferMaterializeTopN<>(logicalTopN.withChildren(ImmutableList.of(children.get(0))),
                deferMaterializeSlotIds, columnIdSlot, Optional.empty(), Optional.empty(), children.get(0));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        LogicalDeferMaterializeTopN<?> that = (LogicalDeferMaterializeTopN<?>) o;
        return Objects.equals(logicalTopN, that.logicalTopN) && Objects.equals(deferMaterializeSlotIds,
                that.deferMaterializeSlotIds) && Objects.equals(columnIdSlot, that.columnIdSlot);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), logicalTopN, deferMaterializeSlotIds, columnIdSlot);
    }

    @Override
    public String toString() {
        return Utils.toSqlString("LogicalDeferMaterializeTopN[" + id.asInt() + "]",
                "logicalTopN", logicalTopN,
                "deferMaterializeSlotIds", deferMaterializeSlotIds,
                "columnIdSlot", columnIdSlot
        );
    }
}