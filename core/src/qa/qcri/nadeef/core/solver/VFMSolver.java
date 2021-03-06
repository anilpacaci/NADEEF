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

package qa.qcri.nadeef.core.solver;

import com.google.common.collect.*;
import qa.qcri.nadeef.core.datamodel.Cell;
import qa.qcri.nadeef.core.datamodel.Fix;
import qa.qcri.nadeef.core.datamodel.Operation;
import qa.qcri.nadeef.core.exceptions.NadeefDatabaseException;
import qa.qcri.nadeef.core.utils.sql.ValueHelper;
import qa.qcri.nadeef.tools.Logger;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

/**
 * Value Frequency Solver. It is a solver to deal with EQ / NEQ only
 * {@link qa.qcri.nadeef.core.datamodel.Fix}. The implementation is by counting the
 * frequency of values and set the new value with the highest count.
 */
public class VFMSolver {

    private static Logger tracer = Logger.getLogger(VFMSolver.class);


    public List<Fix> solve(Collection<Fix> repairContext) {
        HashSet<Cell> cells = Sets.newHashSet();
        HashMap<String, Integer> countMap = Maps.newHashMap();
//        Object original=null;
        for (Fix fix : repairContext) {

//            original=fix.getLeft().getValue();
            if (fix.isRightConstant()) {
                if(fix.getOperation().equals(Operation.EQ))
                    createOrAdd(countMap, fix.getRightValue());
                else
                    createOrDeduct(countMap,fix.getRightValue());
                cells.add(fix.getLeft());
            }
            // not in use
            else {
                cells.add(fix.getLeft());
                createOrAdd(countMap, fix.getLeft().getValue());
                cells.add(fix.getRight());
                createOrAdd(countMap, fix.getRight().getValue());
            }
        }
        java.util.Iterator<Cell> iter=cells.iterator();
        String original=iter.next().getValue().toString();
        int maxCount = 0; String solution=original;
        if(countMap.containsKey(original) && countMap.get(original)<0) {
            solution = "";
            Cell dirtyCell = cells.iterator().next();
            String tableName = dirtyCell.getColumn().getTableName();
            String cleanTableName = tableName.replace("NOISE", "CLEAN").replace("noise", "clean");
            String attributeName = dirtyCell.getColumn().getColumnName();

            try {
                List<String> distintValues = ValueHelper.getInstance().getDistinctValues(cleanTableName, attributeName);
                for(String domainValue : distintValues) {
                    if(!countMap.containsKey(domainValue)) {
                        // this is a value not used in domain. Does not have any constraint saying it should ne NEQ
                        solution = domainValue;
                        break;
                    }
                }
            } catch (NadeefDatabaseException e) {
                tracer.error("Distinct values could NOT be read, suggest empty string", e);
                solution = "";
            }
        }


        for(String o:countMap.keySet()){
            if(countMap.get(o)>=maxCount){
                maxCount=countMap.get(o);
                solution=o;
            }
        }
        List<Fix> result = Lists.newArrayList();

        Fix.Builder builder = new Fix.Builder();
            for (Cell cell : cells) {
                result.add(
                    builder
                        .left(cell)
                        .right(solution)
                        .op(Operation.EQ)
                        .build()
                );
            }

        return result;


        // executing VFM
        // we start with counting both the EQ and NEQ.
        // TODO: we don't know how to deal with NEQ.
        // When there are only NEQ, we need to assign an variable class.


        /*for (Fix fix : repairContext) {
            if (fix.isRightConstant()) {
                createOrAdd(countMap, fix.getRightValue());
                cells.add(fix.getLeft());
            } else {
                cells.add(fix.getLeft());
                createOrAdd(countMap, fix.getLeft().getValue());
                cells.add(fix.getRight());
                createOrAdd(countMap, fix.getRight().getValue());
            }
        }

        // count all the cells in the context.
        for (Cell cell : cells)
            createOrAdd(countMap, cell.getValue());

        // pick the highest count first
        int maxCount = 0;
        for (Integer count : countMap.values())
            maxCount = Math.max(count, maxCount);

        Object target = null;
        if (cells.size() == 1) {
            // A very special case when there is only one cell involved,
            // in that case we pick the highest value which is not equal to original value.
            Object original = cells.iterator().next().getValue();
            for (Map.Entry<Object, Integer> entry : countMap.entrySet()) {
                if (entry.getValue().equals(maxCount) && !entry.getKey().equals(original))
                    target = entry.getKey();
            }
        } else {
            // In normal cases we just pick the highest occurrence one.
            for (Map.Entry<Object, Integer> entry : countMap.entrySet())
                if (entry.getValue().equals(maxCount))
                    target = entry.getKey();
        }

        List<Fix> result = Lists.newArrayList();
        if (target != null) {
            Fix.Builder builder = new Fix.Builder();
            for (Cell cell : cells) {
                if (cell.getValue().equals(target))
                    continue;
                result.add(
                    builder
                        .left(cell)
                        .right(target)
                        .op(Operation.EQ)
                        .build()
                );
            }
        }
        return result;*/
    }

    private void createOrAdd(HashMap<String, Integer> map, String key) {
        if (map.containsKey(key)) {
            int count = map.get(key);
            map.put(key, count + 1);
        } else {
            map.put(key, 1);
        }
    }
    private void createOrDeduct(HashMap<String, Integer> map, String key) {
        if (map.containsKey(key)) {
            int count = map.get(key);
            map.put(key, count - 1);
        } else {
            map.put(key, -1);
        }
    }
}
