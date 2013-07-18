/*
 * QCRI, NADEEF LICENSE
 * NADEEF is an extensible, generalized and easy-to-deploy data cleaning platform built at QCRI.
 * NADEEF means "Clean" in Arabic
 *
 * Copyright (c) 2011-2013, Qatar Foundation for Education, Science and Community Development (on
 * behalf of Qatar Computing Research Institute) having its principle place of business in Doha,
 * Qatar with the registered address P.O box 5825 Doha, Qatar (hereinafter referred to as "QCRI")
 *
 * NADEEF has patent pending nevertheless the following is granted.
 * NADEEF is released under the terms of the MIT License, (http://opensource.org/licenses/MIT).
 */

package qa.qcri.nadeef.test.udf;/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */
import qa.qcri.nadeef.core.datamodel.*;
import qa.qcri.nadeef.core.datamodel.IteratorStream;

import java.util.*;


public class MyRule4 extends PairTupleRule {
    protected List<Column> leftHandSide = new ArrayList();
    protected List<Column> rightHandSide = new ArrayList();

    public MyRule4() {}

    @Override
    public void initialize(String id, List<String> tableNames) {
        super.initialize(id, tableNames);
        leftHandSide.add(new Column("csv_hospital_30k.zipcode"));

        rightHandSide.add(new Column("csv_hospital_30k.city"));

    }

    /**
     * Default horizontal scope operation.
     * @param tables input tuple collections.
     * @return filtered tuple collection.
     */
    @Override
    public Collection<Table> horizontalScope(
        Collection<Table> tables
    ) {
        tables.iterator().next().project(leftHandSide).project(rightHandSide);
        return tables;
    }

    /**
     * Default block operation.
     * @param tables a collection of tables.
     * @return a collection of blocked tables.
     */
    @Override
    public Collection<Table> block(Collection<Table> tables) {
        Table table = tables.iterator().next();
        Collection<Table> groupResult = table.groupOn(leftHandSide);
        return groupResult;
    }

    /**
     * Default iterator operation.
     *
     * @param tables input tables
     */
    @Override
    public void iterator(Collection<Table> tables, IteratorStream<TuplePair> iteratorStream) {
        Table table = tables.iterator().next();
        table.orderBy(rightHandSide);
        int pos1 = 0, pos2 = 0;
        boolean findViolation = false;

        // ---------------------------------------------------
        // two pointer loop via the block. Linear scan
        // ---------------------------------------------------
        while (pos1 < table.size()) {
            for (pos2 = pos1 + 1; pos2 < table.size(); pos2 ++) {
                Tuple left = table.get(pos1);
                Tuple right = table.get(pos2);
                findViolation = !left.hasSameValue(right);

                // generates all the violations between pos1 - pos2.
                if (findViolation) {
                    for (int i = pos1; i < pos2; i ++) {
                        for (int j = pos2; j < table.size(); j++) {
                            TuplePair pair = new TuplePair(table.get(i), table.get(j));
                            iteratorStream.put(pair);
                        }
                    }
                    break;
                }
            }
            pos1 = pos2;
        }
    }

    /**
     * Detect method.
     * @param tuplePair tuple pair.
     * @return violation set.
     */
    @Override
    public Collection<Violation> detect(TuplePair tuplePair) {
        List<Violation> result = new ArrayList();
        Tuple left = tuplePair.getLeft();
        Tuple right = tuplePair.getRight();
        Violation violation = new Violation(ruleName);
        violation.addTuple(left);
        violation.addTuple(right);
        result.add(violation);
        return result;
    }

    /**
     * Repair of this rule.
     *
     * @param violation violation input.
     * @return a candidate fix.
     */
    @Override
    public Collection<Fix> repair(Violation violation) {
        List<Fix> result = new ArrayList();
        Collection<Cell> cells = violation.getCells();
        HashMap<Column, Cell> candidates = new HashMap<Column, Cell>();
        int vid = violation.getVid();
        Fix fix;
        Fix.Builder builder = new Fix.Builder(violation);
        for (Cell cell : cells) {
            Column column = cell.getColumn();
            if (rightHandSide.contains(column)) {
                if (candidates.containsKey(column)) {
                    // if the right hand is already found out in another tuple
                    Cell right = candidates.get(column);
                    fix = builder.left(cell).right(right).build();
                    result.add(fix);
                } else {
                    // it is the first time of this cell shown up, put it in the
                    // candidate and wait for the next one shown up.
                    candidates.put(column, cell);
                }
            }
        }
        return result;
    }
}