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
import com.google.common.collect.Maps;
import qa.qcri.nadeef.core.datamodel.*;
import qa.qcri.nadeef.core.exceptions.NadeefClassifierException;
import qa.qcri.nadeef.core.utils.AuditManager;
import qa.qcri.nadeef.core.utils.ConsistencyManager;
import qa.qcri.nadeef.core.utils.RankingManager;
import qa.qcri.nadeef.core.utils.sql.*;
import qa.qcri.nadeef.core.utils.user.GroundTruth;
import qa.qcri.nadeef.tools.DBConfig;
import qa.qcri.nadeef.tools.Logger;
import qa.qcri.nadeef.tools.Metrics;
import qa.qcri.nadeef.tools.PerfReport;
import qa.qcri.nadeef.tools.sql.SQLDialect;

import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * @author y997zhan, apacaci
 *         Repairs database using the ground truth, until there are no more violations.
 *         Counts the total number of user interactions needed
 */
public class GuidedRepair
    extends Operator<Optional, Collection<TrainingInstance>> {

    private static final int TRAINING_SET_THRESHOLD = 100;
    private static final double CLASSIFIER_ACCURACY_THRESHOLD = 0.75;
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
        DBConfig sourceDBConfig = getCurrentContext().getConnectionPool().getSourceDBConfig();
        DBConnectionPool sourceConnectionPool = getCurrentContext().getConnectionPool();
        SQLDialect dialect = sourceDBConfig.getDialect();
        SQLDialectBase dialectManager =
            SQLDialectFactory.getDialectManagerInstance(dialect);

        List<TrainingInstance> trainingInstances = new ArrayList<>();

        int globalUserInteractionCount = 0;
        int hitCount = 0;

        // TODO: WARN: XXX: find a better way to pass clean table name
        String dirtyTableName = (String) getCurrentContext().getRule().getTableNames().get(0);
        Schema dirtyTableSchema = DBMetaDataTool.getSchema(sourceDBConfig, dirtyTableName);
        String cleanTableName = dirtyTableName.replace("NOISE", "CLEAN").replace("noise", "clean");

        RankingManager rankingManager = new RankingManager(getCurrentContext(), dirtyTableName, cleanTableName);
        AuditManager auditManager = new AuditManager(getCurrentContext());

        GroundTruth userSimulation = new GroundTruth(this.getCurrentContext(), cleanTableName, dirtyTableSchema);

        int offset = 0;

        try {
            while (true) {
                // initialize all counters what we use
                int hitPerAttribute = 0;
                int userInteractionPerAttribute = 0;

                //when next group, offset may still be the last offset of last group, reset it to 0
                offset = 0;
                RepairGroup topGroup = rankingManager.getTopGroup();

                if (topGroup == null) {
                    // no more repair groups. break
                    break;
                }

                topGroup.populateFixByVOI();

                while (topGroup.hasNext(offset)) {
                    Fix solution = topGroup.getTopFix(offset);

                    if (auditManager.isAlreadyUpdated(solution.getLeft())) {
                        // if cell is already udpated, we do not update it again. We directly skip it
                        offset++;
                        continue;
                    }

                    int tupleID = solution.getLeft().getTid();
                    String attribute = solution.getLeft().getColumn().getColumnName();

                    // user interaction, simulate user interaction by checking from clean dataset, ground truth
                    Tuple dirtyTuple = DBConnectionHelper.getDatabaseTuple(sourceConnectionPool, dirtyTableName, dirtyTableSchema, tupleID);
                    Object dirtyValue = dirtyTuple.getCell(attribute).getValue();

                    Object solutionValue;
                    // GurobiSolver returns numerical answers in form of Double. We need to distinguish true integers
                    if (dirtyValue instanceof Integer) {
                        solutionValue = Math.round(Double.parseDouble(solution.getRightValue()));
                    } else {
                        solutionValue = solution.getRightValue();
                    }

                    // will be used to update model
                    double similarityScore = Metrics.getEqual(dirtyValue.toString(), solutionValue.toString());
                    TrainingInstance newTrainingInstance = new TrainingInstance(null, dirtyTuple, attribute, solutionValue.toString(), similarityScore);

                    boolean isHit = userSimulation.acceptFix(solution);

                    if (isHit) {
                        // HIT :)) dirty cell correctly identified, now update database, reset the offset
                        offset = 0;

                        // increase hit count
                        hitCount++;

                        auditManager.applyFix(solution, null);

                        // add positive training instance
                        newTrainingInstance = new TrainingInstance(TrainingInstance.Label.YES, dirtyTuple, attribute, solutionValue.toString(), similarityScore);

                        // call ConsistencyManager to recompute violatios
                        Cell updatedCell = new Cell.Builder().tid(tupleID).column(new Column(dirtyTableName, attribute)).value(solutionValue).build();
                        // remove existing violations and find new ones
                        Set<Integer> affectedTuples = ConsistencyManager.getInstance().checkConsistency(getCurrentContext(), updatedCell);

                        topGroup.populateFixByVOI();
                    } else {
                        // just increase the offset to retrieve the nextrepaircell
                        offset++;
                        if (offset > 20) {
                            //System.out.println("Count:" + userInteractionCount + " Offset:" + offset + " tupleid:" + tupleID + " attribute:" + attribute + " currentValue:" + dirtyTuple.getCell(attribute).getValue());
                        }
                        // add negative training instance
                        newTrainingInstance = new TrainingInstance(TrainingInstance.Label.NO, dirtyTuple, attribute, solutionValue.toString(), similarityScore);
                    }

                    trainingInstances.add(newTrainingInstance);
                    userInteractionPerAttribute++;
                    globalUserInteractionCount++;


                    Integer violationCount = ConsistencyManager.getInstance().countViolation(getCurrentContext());
                    // output interaction count and # of violations
                    System.out.println("# of violations: " + violationCount + " Interaction count: " + globalUserInteractionCount);

                }
            }
        } catch (Exception e) {
            tracer.error("Guided repair could NOT be completed due to SQL Expcetion: ", e);
            throw e;
        }

        long elapseTime = stopwatch.elapsed(TimeUnit.MILLISECONDS);

        PerfReport.appendMetric(PerfReport.Metric.RepairCallTime, elapseTime);
        PerfReport.appendMetric(PerfReport.Metric.UserInteractionHITCount, hitCount);
        PerfReport.appendMetric(PerfReport.Metric.UserInteractionCount, globalUserInteractionCount);
        stopwatch.stop();
        return trainingInstances;
    }

}
