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
import qa.qcri.nadeef.core.utils.UpdateManager;
import qa.qcri.nadeef.core.utils.sql.DBConnectionPool;
import qa.qcri.nadeef.core.utils.sql.SQLDialectBase;
import qa.qcri.nadeef.core.utils.sql.SQLDialectFactory;
import qa.qcri.nadeef.tools.DBConfig;
import qa.qcri.nadeef.tools.Logger;
import qa.qcri.nadeef.tools.PerfReport;
import qa.qcri.nadeef.tools.sql.SQLDialect;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * @author y997zhan, apacaci
 *         Repairs database using the ground truth, until there are no more violations.
 *         Counts the total number of user interactions needed
 */
public class GuidedRepair
    extends Operator<Optional, Integer> {

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
    public Integer execute(Optional emptyInput)
        throws Exception {
        Stopwatch stopwatch = Stopwatch.createStarted();
        Rule rule = getCurrentContext().getRule();
        DBConfig dbConfig = getCurrentContext().getConnectionPool().getSourceDBConfig();
        SQLDialect dialect = dbConfig.getDialect();
        SQLDialectBase dialectManager =
            SQLDialectFactory.getDialectManagerInstance(dialect);


        int userInteractionCount = 0;


        // TODO: WARN: XXX: find a better way to pass clean table name
        String dirtyTableName = (String) getCurrentContext().getRule().getTableNames().get(0);
        String cleanTableName = dirtyTableName.replace("NOISE", "CLEAN").replace("noise", "clean");

        Connection conn = DBConnectionPool.createConnection(dbConfig);
        Statement stat = conn.createStatement();

        try {
            while (true) {

                ResultSet rs = stat.executeQuery(dialectManager.nextRepairCell(NadeefConfiguration.getViolationTableName(), NadeefConfiguration.getCellDegreeViewName(), NadeefConfiguration.getTupleDegreeViewName()));
                if (rs.next()) {
                    int tupleID = rs.getInt("tupleid");
                    String attributeName = rs.getString("attribute");
                    rs.close();

                    Object originalValue, currentValue;

                    // user interaction, simulate user interaction by checking from clean dataset, ground truth
                    rs = stat.executeQuery(dialectManager.selectCell(cleanTableName, tupleID, attributeName));
                    if(rs.next()) {
                        originalValue = rs.getObject(attributeName);
                        rs.close();
                    } else {
                        throw new Exception("Clean Table could NOT be read on tuple: " + tupleID);
                    }

                    rs = stat.executeQuery(dialectManager.selectCell(dirtyTableName, tupleID, attributeName));
                    if(rs.next()) {
                        currentValue = rs.getObject(attributeName);
                        rs.close();
                    } else {
                        throw new Exception("Dirty Table could NOT be read on tuple: " + tupleID);
                    }


                    if (!originalValue.equals(currentValue)) {
                        // HIT :)) dirty cell correctly identified, now update database

                        String updateCellSQL = new StringBuilder("UPDATE ").append(dirtyTableName).append(" SET ").append(attributeName).append(" = ?").append(" where tid = ?").toString();
                        PreparedStatement updateStatement = conn.prepareStatement(updateCellSQL);
                        updateStatement.setObject(1, originalValue);
                        updateStatement.setInt(2, tupleID);

                        int res = updateStatement.executeUpdate();
                        updateStatement.close();
                        conn.commit();
                        if(res == 0) {
                            // update is not succesfull
                            throw new Exception("Database could NOT be updated to clean value: " + originalValue + " on tuple: " + tupleID);
                        }
                        // call UpdateManager to recompute violatios
                        Cell updatedCell = new Cell.Builder().tid(tupleID).column(new Column(dirtyTableName, attributeName)).value(originalValue).build();
                        // remove existing violations
                        UpdateManager.getInstance().removeViolations(updatedCell, getCurrentContext());
                        // find new violations
                        UpdateManager.getInstance().findNewViolations(updatedCell, getCurrentContext());
                    }

                    userInteractionCount++;
                    continue;
                } else {
                    // there is no more violation, cells involved in violation, so break the loop
                    break;
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
        stopwatch.stop();
        return userInteractionCount;
    }
}
