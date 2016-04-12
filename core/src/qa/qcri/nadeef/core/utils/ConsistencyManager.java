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

package qa.qcri.nadeef.core.utils;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import qa.qcri.nadeef.core.datamodel.*;
import qa.qcri.nadeef.core.pipeline.DirectIteratorResultHandler;
import qa.qcri.nadeef.core.pipeline.ExecutionContext;
import qa.qcri.nadeef.core.pipeline.FixExport;
import qa.qcri.nadeef.core.utils.sql.DBConnectionPool;
import qa.qcri.nadeef.tools.Logger;

import java.sql.*;
import java.util.*;
import java.util.concurrent.ConcurrentMap;

/**
 * Created by y997zhan on 3/28/16.
 * <p>
 * This is similar to ConsistencyManager in Guided-Data Repair. After every repair to target database, it checks whether existing vilations are still valid and there are any more violations
 */
public class ConsistencyManager {

    private static Logger tracer = Logger.getLogger(ConsistencyManager.class);


    private static ConsistencyManager instance;

    private HashMap<String, Rule> ruleMap;
    private List<Rule> ruleList;

    private ConsistencyManager() {
        ruleMap = new HashMap<>();
        ruleList = new ArrayList<Rule>();
    }

    public static ConsistencyManager getInstance() {
        if (instance == null) {
            instance = new ConsistencyManager();
        }

        return instance;
    }

    public void addRule(Rule rule) {
        // build a hashmap from tuples attributes and rules
        ruleList.add(rule);
        ruleMap.put(rule.getRuleName(), rule);
    }

    public Set<Integer> checkConsistency(ExecutionContext context, Cell updatedCell) throws Exception {
        Set<Integer> affectedCells = removeViolations(updatedCell, context);
        affectedCells.addAll(findNewViolations(updatedCell, context));

        return affectedCells;
    }

    public Set<Integer> removeViolations(Cell updatedCell, ExecutionContext context) throws SQLException, IllegalAccessException, InstantiationException, ClassNotFoundException {
        DBConnectionPool dbConnectionPool = context.getConnectionPool();

        Set<Integer> affectedTuples = Sets.newHashSet();

        // delete all existing violations of this cell
        String violationTableName = NadeefConfiguration.getViolationTableName();
        String repairTableName = NadeefConfiguration.getRepairTableName();

        String effectedCellsSQL = new StringBuilder().append("SELECT c1_tupleid, c2_tupleid FROM ").append(repairTableName).
            append(" WHERE c1_attribute = ? AND c2_attribute = ? AND vid IN (SELECT vid FROM ").
            append(violationTableName).append(" WHERE tupleid = ? AND attribute = ? ) ").toString();

        String deleteRepairSQL = new StringBuilder().append("DELETE FROM ").
            append(repairTableName).
            append(" WHERE vid IN ( SELECT vid FROM ").append(violationTableName).
            append(" where tupleid = ? AND attribute = ? )").
            toString();

        String deleteViolationsSQL = new StringBuilder().append("DELETE FROM ").
            append(violationTableName).
            append(" WHERE vid IN ( SELECT vid FROM ").
            append(violationTableName).
            append(" where tupleid = ? AND attribute = ? )").toString();

        Connection conn = null;
        PreparedStatement stat = null;
        try {
            conn = dbConnectionPool.getNadeefConnection();

            // first retrieve all effected tupleids
            stat = conn.prepareStatement(effectedCellsSQL);
            stat.setString(1, updatedCell.getColumn().getColumnName());
            stat.setString(2, updatedCell.getColumn().getColumnName());
            stat.setInt(3, updatedCell.getTid());
            stat.setString(4, updatedCell.getColumn().getColumnName());
            ResultSet resultSet = stat.executeQuery();
            while(resultSet.next()) {
                int tupleID1 = resultSet.getInt(1);
                int tupleID2 = resultSet.getInt(2);
                affectedTuples.add(tupleID1);
                affectedTuples.add(tupleID2);
            }
            resultSet.close();

            stat = conn.prepareStatement(deleteRepairSQL);
            stat.setInt(1, updatedCell.getTid());
            stat.setString(2, updatedCell.getColumn().getColumnName());
            int res = stat.executeUpdate();

            stat = conn.prepareStatement(deleteViolationsSQL);
            stat.setInt(1, updatedCell.getTid());
            stat.setString(2, updatedCell.getColumn().getColumnName());
            res = stat.executeUpdate();

            conn.commit();
            tracer.fine(res + " violations have been deleted.");

        } catch(Exception e) {
            tracer.error("Violation could NOT be deleted", e);
        } finally {
            if (stat != null && !stat.isClosed()) {
                stat.close();
            }

            if (conn != null && !conn.isClosed()) {
                conn.close();
            }
        }

        // remove the original tuple id
        affectedTuples.remove(updatedCell.getTid());
        return affectedTuples;
    }

    public Set<Integer> findNewViolations(Cell updatedCell, ExecutionContext context) throws Exception {
        // check if this new cell creates a new violation
        Set<Integer> affectedCells = Sets.newHashSet();

        NonBlockingCollectionIterator<Violation> outputIterator = new NonBlockingCollectionIterator<>();
        Collection<Collection<Fix>> newRepairs = Lists.newArrayList();

        for (Rule rule : ruleList) {

            String tableName = updatedCell.getColumn().getTableName();

            // create a single block from the whole table
            List<Table> tableList = new ArrayList<>();
            tableList.add(new SQLTable(tableName, context.getConnectionPool()));

            // generate newly added tuple list, consists of single tuple - last updated one
            ConcurrentMap<String, HashSet<Integer>> newTuples = Maps.newConcurrentMap();
            newTuples.put(tableName, Sets.newHashSet(updatedCell.getTid()));

            DirectIteratorResultHandler directIteratorResultHandler = new DirectIteratorResultHandler(rule, outputIterator);

            // call the rule iterator on whole table block and single new tuple list
            rule.iterator(tableList, newTuples, directIteratorResultHandler);

            //now outputIterator contains newly detected violation. We just need to serialize them into database

        }


        Connection conn = null;
        PreparedStatement stat = null;
        DBConnectionPool connectionPool = context.getConnectionPool();
        try {
            int vid = Violations.generateViolationId(connectionPool);

            conn = connectionPool.getNadeefConnection();
            stat = conn.prepareStatement("INSERT INTO VIOLATION VALUES (?, ?, ?, ?, ?, ?)");
            int count = 0;
            while (outputIterator.hasNext()) {
                Violation violation = outputIterator.next();
                violation.setVid(vid);
                count++;
                Collection<Cell> cells = violation.getCells();
                for (Cell cell : cells) {
                    // skip the tuple id
                    if (cell.hasColumnName("tid")) {
                        continue;
                    }
                    stat.setInt(1, vid);
                    stat.setString(2, violation.getRuleId());
                    stat.setString(3, cell.getColumn().getTableName());
                    stat.setInt(4, cell.getTid());
                    stat.setString(5, cell.getColumn().getColumnName());
                    Object value = cell.getValue();
                    if (value == null) {
                        stat.setString(6, null);
                    } else {
                        stat.setString(6, value.toString());
                    }
                    stat.addBatch();
                }

                if (count % 4096 == 0) {
                    stat.executeBatch();
                }

                // generate fixes for this violation
                Rule rule = ruleMap.get(violation.getRuleId());

                Collection fixes = rule.repair(violation);
                newRepairs.add(fixes);

                vid++;
            }
            stat.executeBatch();
            conn.commit();

        } finally {
            if (stat != null) {
                stat.close();
            }
            if (conn != null) {
                conn.close();
            }
        }

        // now insert newRepairs into repair table
        Statement statement = null;
        try {
            conn = connectionPool.getNadeefConnection();
            statement = conn.createStatement();
            int id = Fixes.generateFixId(connectionPool);
            for (Collection<Fix> fixes : newRepairs) {
                for (Fix fix : fixes) {
                    String sql = FixExport.getSQLInsert(id, fix);
                    statement.addBatch(sql);

                    // add tupleid to affected cells
                    if(fix.getLeft().getColumn().equals(updatedCell.getColumn()) && fix.getRight().getColumn().equals(updatedCell.getColumn())) {
                        affectedCells.add(fix.getLeft().getTid());
                        affectedCells.add(fix.getRight().getTid());
                    }
                }
                id ++;
            }
            statement.executeBatch();
            conn.commit();

        } finally {
            if (statement != null) {
                statement.close();
            }
            if (conn != null) {
                conn.close();
            }
        }

        // remove the original tupleid
        affectedCells.remove(updatedCell.getTid());
        return affectedCells;

    }

}
