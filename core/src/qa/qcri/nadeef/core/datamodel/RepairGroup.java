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
import qa.qcri.nadeef.core.exceptions.NadeefClassifierException;
import qa.qcri.nadeef.core.exceptions.NadeefDatabaseException;
import qa.qcri.nadeef.core.pipeline.ExecutionContext;
import qa.qcri.nadeef.core.solver.SuggestedRepairSolver;
import qa.qcri.nadeef.core.utils.Fixes;
import qa.qcri.nadeef.core.utils.classification.ClassifierBase;
import qa.qcri.nadeef.core.utils.sql.DBConnectionPool;
import qa.qcri.nadeef.core.utils.sql.DBMetaDataTool;
import qa.qcri.nadeef.core.utils.sql.SQLDialectBase;
import qa.qcri.nadeef.core.utils.sql.SQLDialectFactory;
import qa.qcri.nadeef.tools.DBConfig;
import qa.qcri.nadeef.tools.Logger;
import qa.qcri.nadeef.tools.Metrics;
import qa.qcri.nadeef.tools.sql.SQLDialect;

import java.sql.*;
import java.util.*;

/**
 * Created by y997zhan on 4/7/16.
 */
public class RepairGroup implements Comparable {

    private static Logger tracer = Logger.getLogger(RepairGroup.class);

    private ExecutionContext context;
    private Column attributeColumn;
    private String dirtyTableName;

    private Map<Fix, Double> scoreMap;
    private List<Map.Entry<Fix, Double>> sortedFixByScore;

    private List<Fix> solutionList;
    private Map<Fix, TrainingInstance> trainingInstanceMap;

    private Schema databaseSchema;

    public Column getColumn(){
        return this.attributeColumn;
    }

    public RepairGroup(Column attributeColumn, String dirtyTableName, Schema databaseSchema, ExecutionContext context) {
        this.attributeColumn = attributeColumn;
        this.dirtyTableName = dirtyTableName;
        this.context = context;

        this.scoreMap = Maps.newHashMap();
        this.sortedFixByScore = Lists.newArrayList();
        this.solutionList = Lists.newArrayList();
        this.trainingInstanceMap = Maps.newHashMap();

        this.databaseSchema = databaseSchema;

    }

    public double getTotalScore() {
        return scoreMap.values().stream().mapToDouble(Double::doubleValue).sum();
    }

    public void populateFixByVOI() throws NadeefDatabaseException {
        scoreMap.clear();
        sortedFixByScore.clear();

        DBConfig dbConfig = this.context.getConnectionPool().getNadeefConfig();
        Connection conn = null;

        // find new cells
        String selectCellsQuery = new StringBuilder().append("SELECT DISTINCT tupleid FROM ").append(NadeefConfiguration.getViolationTableName()).append(" WHERE attribute = ?").toString();

        try {
            conn = this.context.getConnectionPool().getNadeefConnection();
            PreparedStatement statement = conn.prepareStatement(selectCellsQuery);
            statement.setString(1, this.attributeColumn.getColumnName());
            ResultSet rs = statement.executeQuery();
            while(rs.next()) {
                int tupleid = rs.getInt(1);
                Tuple tuple = getDatabaseTuple(this.dirtyTableName, tupleid);
                Cell cell = tuple.getCell(this.attributeColumn.getColumnName());
                Collection<Fix> fixes = getFixesOfCell(cell);
                Fix solution = new SuggestedRepairSolver().solve(fixes).iterator().next();

                if(solution.getRightValue().isEmpty()) {
                    // this happens in case of all NEQs, it means v-repair. We need to suggest a value from domain
                }

                this.solutionList.add(solution);
                this.trainingInstanceMap.put(solution, new TrainingInstance(null, tuple,this.attributeColumn.getColumnName(),  solution.getRightValue(), 0));

                double score = calculateVOIScore(solution, fixes);
                this.scoreMap.put(solution, score);
            }
            rs.close();
            statement.close();// sort VOI into sortedList, in descending order
            conn.close();

            rankByScore();

        } catch (Exception e) {
            tracer.error("Cells of a group " + this.attributeColumn.getColumnName()+ " could NOT be retrieved", e);
            throw new NadeefDatabaseException(e);
        }
    }

    public void populateFixByEntropy(ClassifierBase classifier) throws NadeefDatabaseException {
        scoreMap.clear();
        sortedFixByScore.clear();

        DBConfig nadeefDBConfig = this.context.getConnectionPool().getNadeefConfig();
        Connection conn = null;

        // find new cells
        String selectCellsQuery = new StringBuilder().append("SELECT DISTINCT tupleid FROM ").append(NadeefConfiguration.getViolationTableName()).append(" WHERE attribute = ?").toString();

        try {
            conn = this.context.getConnectionPool().getNadeefConnection();
            PreparedStatement statement = conn.prepareStatement(selectCellsQuery);
            statement.setString(1, this.attributeColumn.getColumnName());
            ResultSet rs = statement.executeQuery();
            while(rs.next()) {
                int tupleid = rs.getInt(1);
                Tuple tuple = getDatabaseTuple(this.dirtyTableName, tupleid);
                Cell cell = tuple.getCell(this.attributeColumn.getColumnName());
                Collection<Fix> fixes = getFixesOfCell(cell);
                Fix solution = new SuggestedRepairSolver().solve(fixes).iterator().next();

                if(solution.getRightValue().isEmpty()) {
                    // this happens in case of all NEQs, it means v-repair. We need to suggest a value from domain

                }

                double similartyScore = Metrics.getEqual(cell.getValue().toString(), solution.getRightValue());
                this.solutionList.add(solution);
                this.trainingInstanceMap.put(solution, new TrainingInstance(null, tuple,this.attributeColumn.getColumnName(),  solution.getRightValue(), similartyScore));

                double score = calculateEntropyScore(solution, classifier);
                this.scoreMap.put(solution, score);
            }
            rs.close();
            statement.close();// sort VOI into sortedList, in descending order
            conn.close();

            rankByScore();

        } catch (Exception e) {
            tracer.error("Cells of a group " + this.attributeColumn.getColumnName()+ " could NOT be retrieved", e);
            throw new NadeefDatabaseException(e);
        }
    }


    /**
     * Ranks the possible repairs inside group bsaed on VOI
     */
    public void rankByScore() {
        // sort VOI into sortedList, in descending order
        Ordering<Map.Entry<Fix, Double>> orderByScore = new Ordering<Map.Entry<Fix, Double>>() {
            @Override
            public int compare(Map.Entry<Fix, Double> left, Map.Entry<Fix, Double> right) {
                return -left.getValue().compareTo(right.getValue());
            }
        };

        this.sortedFixByScore.addAll(this.scoreMap.entrySet());
        Collections.sort(this.sortedFixByScore, orderByScore);
    }

    /**
     * Retrieve top Fix by VOI
     * @param offset
     * @return <code>null</code> if there is no more Fix
     */
    public Fix getTopFix(int offset) {
        if(offset < this.sortedFixByScore.size())
            return this.sortedFixByScore.get(offset).getKey();
        else
            return null;
    }

    public boolean hasNext(int offset) {
        return offset < this.sortedFixByScore.size();
    }

    private double calculateEntropyScore(Fix fix, ClassifierBase randomForestClassifier) {
        TrainingInstance trainingInstance = this.trainingInstanceMap.get(fix);

        double entropy = 0;

        ClassificationResult result = null;
        try {
            result = randomForestClassifier.getPrediction(trainingInstance);
        } catch (NadeefClassifierException e) {
            e.printStackTrace();
        }

        Collection<Double> distribution = result.getProbabilities();

        for(Double probability : distribution) {
            if(probability == 0) {
                continue;
            }
            entropy -= probability * Math.log(probability);
        }

        return entropy;
    }

    private double calculateVOIScore(Fix fix, Collection<Fix> repairContext) {
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
        DBConfig sourceDBConfig = this.context.getConnectionPool().getSourceDBConfig();
        DBConnectionPool dbConnectionPool = this.context.getConnectionPool();

        SQLDialectBase dialectManager = SQLDialectFactory.getDialectManagerInstance(sourceDBConfig.getDialect());
        Connection conn = null;
        Statement stat = null;
        Object value = null;

        try {
            conn = dbConnectionPool.getSourceConnection();
            stat = conn.createStatement();
            ResultSet rs = stat.executeQuery(dialectManager.selectCell(tableName, tupleID, attribute));
            if(rs.next()) {
                value = rs.getObject(attribute);
                builder.tid(tupleID).column(new Column(tableName, attribute)).value(value);
            }
            rs.close();
        } catch (SQLException e) {
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
     * @param tableName Dirty or Clean Data source. It only works for source databases imported to Nadeef
     * @param tupleID
     * @return
     * @throws SQLException
     * @throws NadeefDatabaseException
     */
    private Tuple getDatabaseTuple(String tableName, int tupleID ) throws SQLException, NadeefDatabaseException{
        Cell.Builder builder = new Cell.Builder();
        DBConnectionPool connectionPool = this.context.getConnectionPool();
        DBConfig sourceDBConfig = this.context.getConnectionPool().getSourceDBConfig();

        SQLDialectBase dialectManager = SQLDialectFactory.getDialectManagerInstance(sourceDBConfig.getDialect());
        Connection conn = null;
        Statement stat = null;
        Tuple tuple = null;
        Schema schema = null;

        try {
            schema = DBMetaDataTool.getSchema(sourceDBConfig, tableName);
            conn = connectionPool.getSourceConnection();
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

    private Collection<Fix> getFixesOfCell(Cell cell) throws NadeefDatabaseException, SQLException {
        DBConfig nadeefDBConfig = this.context.getConnectionPool().getNadeefConfig();
        DBConnectionPool dbConnectionPool = this.context.getConnectionPool();
        String repairTableName = NadeefConfiguration.getRepairTableName();

        String sql = new StringBuilder().append("SELECT * FROM ").append(repairTableName).append(" WHERE ( c1_tupleid = ? OR c2_tupleid = ? ) AND ( c1_attribute = ? OR c2_attribute = ? ) ").toString();

        Connection conn = null;
        PreparedStatement stat = null;

        ResultSet resultSet = null;
        SQLDialect dialect = nadeefDBConfig.getDialect();
        SQLDialectBase dialectBase =
            SQLDialectBase.createDialectBaseInstance(dialect);
        Collection<Fix> result = null;
        try {
            conn = dbConnectionPool.getNadeefConnection();
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
