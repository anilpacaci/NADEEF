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

package qa.qcri.nadeef.core.datamodel;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import qa.qcri.nadeef.core.exceptions.NadeefDatabaseException;
import qa.qcri.nadeef.core.pipeline.ExecutionContext;
import qa.qcri.nadeef.core.solver.HolisticCleaning;
import qa.qcri.nadeef.core.utils.Fixes;
import qa.qcri.nadeef.core.utils.sql.DBConnectionPool;
import qa.qcri.nadeef.core.utils.sql.SQLDialectBase;
import qa.qcri.nadeef.core.utils.sql.SQLDialectFactory;
import qa.qcri.nadeef.tools.DBConfig;
import qa.qcri.nadeef.tools.Logger;
import qa.qcri.nadeef.tools.sql.SQLDialect;

import java.sql.*;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Created by y997zhan on 4/7/16.
 */
public class RepairGroup implements Comparable {

    private static Logger tracer = Logger.getLogger(RepairGroup.class);

    private ExecutionContext context;
    private String attributeName;
    private String dirtyTableName;

    private Map<Fix, Double> VOIMap;
    private List<Map.Entry<Fix, Double>> sortedFixByVIO;

    public RepairGroup(String attributeName, String dirtyTableName, ExecutionContext context) {
        this.attributeName = attributeName;
        this.dirtyTableName = dirtyTableName;
        this.context = context;

        this.VOIMap = Maps.newHashMap();
        this.sortedFixByVIO = Lists.newArrayList();
    }

    public double getTotalScore() {
        return VOIMap.values().stream().mapToDouble(Double::doubleValue).sum();
    }

    public void populateFix() {
        VOIMap.clear();

        DBConfig dbConfig = this.context.getConnectionPool().getNadeefConfig();
        Connection conn = null;

        // find new cells
        String selectCellsQuery = new StringBuilder().append("SELECT DISTINCT tid FROM ").append(NadeefConfiguration.getViolationTableName()).append(" WHERE attribute = ?").toString();

        try {
            conn = DBConnectionPool.createConnection(dbConfig);
            PreparedStatement statement = conn.prepareStatement(selectCellsQuery);
            statement.setString(1, this.attributeName);
            ResultSet rs = statement.executeQuery();
            while(rs.next()) {
                int tupleid = rs.getInt(1);
                Cell cell = getDatabaseCell(this.dirtyTableName, tupleid, this.attributeName);
                Collection<Fix> fixes = getFixesOfCell(cell);
                Fix solution = new HolisticCleaning(this.context).decide(fixes).iterator().next();

                if(solution.getRightValue().isEmpty()) {
                    // this happens in case of all NEQs, it means v-repair. We need to suggest a value from domain
                }
                double score = calculateVOI(solution, fixes);
                this.VOIMap.put(solution, score);
            }
        // sort VOI into sortedList
            Ordering<Map.Entry<Fix, Double>> orderByVOI = new Ordering<Map.Entry<Fix, Double>>() {
                @Override
                public int compare(Map.Entry<Fix, Double> left, Map.Entry<Fix, Double> right) {
                    return left.getValue().compareTo(right.getValue());
                }
            };

            this.sortedFixByVIO.addAll(this.VOIMap.entrySet());
            Collections.sort(this.sortedFixByVIO, orderByVOI);


        } catch (Exception e) {

        }

    }

    /**
     * Retrieve top Fix by VOI
     * @param offset
     * @return <code>null</code> if there is no more Fix
     */
    public Fix getTopFix(int offset) {
        if(offset < this.sortedFixByVIO.size())
            return this.sortedFixByVIO.get(offset).getKey();
        else
            return null;
    }

    private double calculateVOI(Fix fix, Collection<Fix> repairContext) {
        boolean onlyequality=true;
        for(Fix repair:repairContext){
            if(!repair.getOperation().equals(Operation.EQ) && !repair.getOperation().equals(Operation.NEQ)){
                onlyequality=false; break;
            }
        }
        double result=0.0;
        if(onlyequality) {
            String solution=fix.getRightValue();
            for (Fix repair : repairContext) {
                String rightValue=repair.getRightValue();
                switch (repair.getOperation()){
                    case EQ:
                        if(solution.equals(rightValue)) result++;
                        break;
                    case NEQ:
                        if(!solution.equals(rightValue)) result++;
                        break;
                }
            }
        }
        else{
            double solution=Double.parseDouble(fix.getRightValue());
            for (Fix repair : repairContext) {
                double rightValue=Double.parseDouble(repair.getRightValue());
                switch (repair.getOperation()){
                    case EQ:
                        if(solution==rightValue) result++;
                        break;
                    case LT:
                        if(solution<rightValue) result++;
                        break;
                    case GT:
                        if(solution>rightValue) result++;
                        break;
                    case NEQ:
                        if(solution!=rightValue) result++;
                        break;
                    case LTE:
                        if(solution<=rightValue) result++;
                        break;
                    case GTE:
                        if(solution>=rightValue) result++;
                        break;
                }
            }
        }
        return result;
    }

    /**
     * Reads the value of a cell and constructs a cell object
     * @param tableName Dirty or Clean Data source. It only works for source databases imported to Nadeef
     * @param tupleID
     * @param attribute
     * @return
     * @throws SQLException
     * @throws NadeefDatabaseException
     */
    private Cell getDatabaseCell(String tableName, int tupleID, String attribute) throws SQLException, NadeefDatabaseException {
        Cell.Builder builder = new Cell.Builder();
        DBConfig dbConfig = this.context.getConnectionPool().getSourceDBConfig();

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

    private Collection<Fix> getFixesOfCell(Cell cell) throws NadeefDatabaseException, SQLException {
        DBConfig dbConfig = this.context.getConnectionPool().getNadeefConfig();
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

    @Override
    /**
     * Compares based on the totalVOI. For descending order
     */
    public int compareTo(Object o) {
        Double thisScore = this.getTotalScore();
        Double itsScore = ((RepairGroup) o).getTotalScore();

        return -thisScore.compareTo(itsScore);
    }
}
