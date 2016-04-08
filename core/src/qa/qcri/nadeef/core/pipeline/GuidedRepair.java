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

package qa.qcri.nadeef.core.pipeline;

import com.google.common.base.Optional;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import qa.qcri.nadeef.core.datamodel.*;
import qa.qcri.nadeef.core.exceptions.NadeefDatabaseException;
import qa.qcri.nadeef.core.solver.HolisticCleaning;
import qa.qcri.nadeef.core.utils.AuditManager;
import qa.qcri.nadeef.core.utils.Fixes;
import qa.qcri.nadeef.core.utils.RankingManager;
import qa.qcri.nadeef.core.utils.UpdateManager;
import qa.qcri.nadeef.core.utils.sql.DBConnectionPool;
import qa.qcri.nadeef.core.utils.sql.DBMetaDataTool;
import qa.qcri.nadeef.core.utils.sql.SQLDialectBase;
import qa.qcri.nadeef.core.utils.sql.SQLDialectFactory;
import qa.qcri.nadeef.tools.DBConfig;
import qa.qcri.nadeef.tools.Logger;
import qa.qcri.nadeef.tools.PerfReport;
import qa.qcri.nadeef.tools.sql.SQLDialect;

import java.sql.*;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * @author y997zhan, apacaci
 *         Repairs database using the ground truth, until there are no more violations.
 *         Counts the total number of user interactions needed
 */
public class GuidedRepair
    extends Operator<Optional, Collection<TrainingInstance>> {

    private static Logger tracer = Logger.getLogger(GuidedRepair.class);

    public GuidedRepair(ExecutionContext context) {
        super(context);
    }

    /**
     * Execute the operator.
     *
     * @param emptyInput .
     * @return Number of questions asked to user until there are no more violations.
     */
    @Override
    @SuppressWarnings("unchecked")
    public Collection<TrainingInstance> execute(Optional emptyInput)
        throws Exception {
        Stopwatch stopwatch = Stopwatch.createStarted();

        Rule rule = getCurrentContext().getRule();
        DBConfig dbConfig = getCurrentContext().getConnectionPool().getSourceDBConfig();
        SQLDialect dialect = dbConfig.getDialect();
        SQLDialectBase dialectManager =
            SQLDialectFactory.getDialectManagerInstance(dialect);

        List<TrainingInstance> trainingInstances = new ArrayList<>();

        int userInteractionCount = 0;
        int hitCount = 0;


        // TODO: WARN: XXX: find a better way to pass clean table name
        String dirtyTableName = (String) getCurrentContext().getRule().getTableNames().get(0);
        String cleanTableName = dirtyTableName.replace("NOISE", "CLEAN").replace("noise", "clean");

        RankingManager rankingManager = new RankingManager(getCurrentContext(), dirtyTableName, cleanTableName);
        AuditManager auditManager = new AuditManager(getCurrentContext());

        Connection conn = DBConnectionPool.createConnection(dbConfig);
        Statement stat = conn.createStatement();

        int offset = 0;

        try {
            while (true) {
                //when next group, offset may still be the last offset of last group, reset it to 0
                offset=0;
                RepairGroup topGroup = rankingManager.getTopGroup();
                if(topGroup == null) {
                    // no more repair groups. break
                    break;
                }

                while(topGroup.hasNext(offset)) {
                    Fix solution = topGroup.getTopFix(offset);
                    String suggestedValue=solution.getRightValue();

                    if(auditManager.isAlreadyUpdated(solution.getLeft())) {
                        // if cell is already udpated, we do not update it again. We directly skip it
                        offset++;
                        continue;
                    }

                    int tupleID = solution.getLeft().getTid();
                    String attribute = solution.getLeft().getColumn().getColumnName();
                    Object solutionValue;

                    // user interaction, simulate user interaction by checking from clean dataset, ground truth
                    Tuple dirtyTuple = getDatabaseTuple(dbConfig, dirtyTableName, tupleID);
                    Cell cleanCell = getDatabaseCell(dbConfig, cleanTableName, tupleID, attribute);

                    Object cleanValue = cleanCell.getValue();
                    Object dirtyValue = dirtyTuple.getCell(attribute).getValue();

                    // GurobiSolver returns numerical answers in form of Double. We need to distinguish true integers
                    if(dirtyValue instanceof Integer) {
                        solutionValue = Math.round(Double.parseDouble(solution.getRightValue()));
                    } else {
                        solutionValue = solution.getRightValue();
                    }

                    if (!cleanValue.toString().equals(dirtyValue.toString())) {
                        // HIT :)) dirty cell correctly identified, now update database, reset the offset
                        offset = 0;

                        // increase hit count
                        hitCount++;

                        auditManager.applyFix(solution);

                        // add positive training instance
                        trainingInstances.add(new TrainingInstance(TrainingInstance.Label.YES, dirtyTuple, cleanCell, 0));

                        // call UpdateManager to recompute violatios
                        Cell updatedCell = new Cell.Builder().tid(tupleID).column(new Column(dirtyTableName, attribute)).value(cleanCell.getValue()).build();
                        // remove existing violations
                        UpdateManager.getInstance().removeViolations(updatedCell, getCurrentContext());
                        // find new violations
                        UpdateManager.getInstance().findNewViolations(updatedCell, getCurrentContext());

                        topGroup.populateFix();
                    } else {
                        // just increase the offset to retrieve the nextrepaircell
                        offset++;
                        if(offset > 20) {
                            System.out.println("Count:" + userInteractionCount + " Offset:" + offset + " tupleid:" + tupleID + " attribute:" + attribute + " currentValue:" + dirtyTuple.getCell(attribute).getValue());
                        }
                        // add negative training instance
                        trainingInstances.add(new TrainingInstance(TrainingInstance.Label.NO, dirtyTuple, cleanCell, 0));
                    }
                    userInteractionCount++;
                }
            }
        } catch (Exception e) {
            tracer.error("Guided repair could NOT be completed due to SQL Expcetion: ", e);
            throw e;
        } finally {
            if (stat != null && !stat.isClosed()) {
                stat.close();
            }
            if (conn != null && !conn.isClosed()) {
                conn.close();
            }
        }


        long elapseTime = stopwatch.elapsed(TimeUnit.MILLISECONDS);

        PerfReport.appendMetric(PerfReport.Metric.RepairCallTime, elapseTime);
        PerfReport.appendMetric(PerfReport.Metric.UserInteractionHITCount, hitCount);
        PerfReport.appendMetric(PerfReport.Metric.UserInteractionCount, userInteractionCount);
        stopwatch.stop();
        return trainingInstances;
    }

    /**
     * Reads the value of a cell and constructs a cell object
     * @param dbConfig
     * @param tableName Dirty or Clean Data source. It only works for source databases imported to Nadeef
     * @param tupleID
     * @param attribute
     * @return
     * @throws SQLException
     * @throws NadeefDatabaseException
     */
    private Cell getDatabaseCell(DBConfig dbConfig, String tableName, int tupleID, String attribute) throws SQLException, NadeefDatabaseException {
        Cell.Builder builder = new Cell.Builder();

        SQLDialectBase dialectManager = SQLDialectFactory.getDialectManagerInstance(dbConfig.getDialect());
        Connection conn = null;
        Statement stat = null;
        Object value = null;

        try {
            conn = DBConnectionPool.createConnection(dbConfig);
            stat = conn.createStatement();
            ResultSet rs = stat.executeQuery(dialectManager.selectCell(tableName, tupleID, attribute));
            if(rs.next()) {
                value = rs.getObject(attribute);
                builder.tid(tupleID).column(new Column(tableName, attribute)).value(value);
            }
            rs.close();
        } catch (IllegalAccessException | InstantiationException | SQLException | ClassNotFoundException e) {
            tracer.error("Cell value could NOT be retrieved from database", e);
            throw new NadeefDatabaseException("Cell with tid:" + tupleID + " attribute: " + attribute + "could NOT be read", e);
        } finally {
            if(stat != null && !stat.isClosed()) {
                stat.close();
            }
            if(conn != null && !conn.isClosed()) {
                conn.close();
            }
        }
        return builder.build();
    }

    /**
     * Reads given tuple from database and constructs a Tuple Object
     * @param dbConfig
     * @param tableName Dirty or Clean Data source. It only works for source databases imported to Nadeef
     * @param tupleID
     * @return
     * @throws SQLException
     * @throws NadeefDatabaseException
     */
    private Tuple getDatabaseTuple(DBConfig dbConfig, String tableName, int tupleID ) throws SQLException, NadeefDatabaseException{
        Cell.Builder builder = new Cell.Builder();

        SQLDialectBase dialectManager = SQLDialectFactory.getDialectManagerInstance(dbConfig.getDialect());
        Connection conn = null;
        Statement stat = null;
        Tuple tuple = null;
        Schema schema = null;

        try {
            schema = DBMetaDataTool.getSchema(dbConfig, tableName);
            conn = DBConnectionPool.createConnection(dbConfig);
            stat = conn.createStatement();
            ResultSet rs = stat.executeQuery(dialectManager.selectTuple(tableName, tupleID));
            if(rs.next()) {
                List<byte[]> values = new ArrayList<>();
                for(Column column : schema.getColumns()) {
                    values.add(rs.getBytes(column.getColumnName()));
                }
                tuple = new Tuple(tupleID, schema,values);
            }
            rs.close();
        } catch (Exception e) {
            tracer.error("Tuple could NOT be retrieved from database", e);
            throw new NadeefDatabaseException("Tuple with tid:" + tupleID + " could NOT be read", e);
        } finally {
            if(stat != null && !stat.isClosed()) {
                stat.close();
            }
            if(conn != null && !conn.isClosed()) {
                conn.close();
            }
        }
        return tuple;
    }

    private Collection<Fix> getFixesOfCell(DBConfig dbConfig, Cell cell) throws NadeefDatabaseException, SQLException {
        String repairTableName = NadeefConfiguration.getRepairTableName();

        String sql = new StringBuilder().append("SELECT * FROM ").append(repairTableName).append(" WHERE ( c1_tupleid = ? OR c2_tupleid = ? ) AND ( c1_attribute = ? OR c2_attribute = ? ) ").toString();

        Connection conn = null;
        PreparedStatement stat = null;

        ResultSet resultSet = null;
        SQLDialect dialect = dbConfig.getDialect();
        SQLDialectBase dialectBase =
            SQLDialectBase.createDialectBaseInstance(dialect);
        Collection<Fix> result = null;
        try {
            conn = DBConnectionPool.createConnection(dbConfig, true);
            stat = conn.prepareStatement(sql);
            stat.setObject(1, cell.getTid());
            stat.setObject(2, cell.getTid());
            stat.setObject(3, cell.getColumn().getColumnName());
            stat.setObject(4, cell.getColumn().getColumnName());
            resultSet = stat.executeQuery();
            result = Fixes.fromQuery(resultSet);

        } catch (Exception e) {
            tracer.error("Repairs could NOT be retrieved from database", e);
            throw new NadeefDatabaseException("Repairs of cell with tid:" + cell.getTid() + " attribute:" + cell.getColumn().getColumnName() + " could NOT be read", e);
        } finally {
            if (resultSet != null) {
                resultSet.close();
            }

            if (stat != null) {
                stat.close();
            }

            if (conn != null) {
                conn.close();
            }
        }

        Collection<Fix> processedResults = Fixes.substituteRHS(cell, result);
        return processedResults;
    }
}
