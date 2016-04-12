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
import qa.qcri.nadeef.core.exceptions.NadeefDatabaseException;
import qa.qcri.nadeef.core.utils.AuditManager;
import qa.qcri.nadeef.core.utils.ConsistencyManager;
import qa.qcri.nadeef.core.utils.RankingManager;
import qa.qcri.nadeef.core.utils.classification.ClassifierBase;
import qa.qcri.nadeef.core.utils.classification.J48Classifier;
import qa.qcri.nadeef.core.utils.classification.RandomForestClassifier;
import qa.qcri.nadeef.core.utils.sql.*;
import qa.qcri.nadeef.core.utils.user.GroundTruth;
import qa.qcri.nadeef.tools.DBConfig;
import qa.qcri.nadeef.tools.Logger;
import qa.qcri.nadeef.tools.Metrics;
import qa.qcri.nadeef.tools.PerfReport;
import qa.qcri.nadeef.tools.sql.SQLDialect;
import weka.classifiers.Classifier;

import java.sql.*;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
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
        DBConfig sourceDBConfig = getCurrentContext().getConnectionPool().getSourceDBConfig();
        DBConnectionPool sourceConnectionPool = getCurrentContext().getConnectionPool();
        SQLDialect dialect = sourceDBConfig.getDialect();
        SQLDialectBase dialectManager =
            SQLDialectFactory.getDialectManagerInstance(dialect);

        List<TrainingInstance> trainingInstances = new ArrayList<>();

        Map<String, RandomForestClassifier> classifierMap = Maps.newHashMap();

        int userInteractionCount = 0;
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
                int interactionPerAttribute = 0;
                int predictedHitPerAttribute = 0;
                int falsePredictedHITPerAttribute = 0;
                int predictedNOTHitPerAttribute = 0;
                int falsePredictedNOTHitPerAttribute = 0;

                //when next group, offset may still be the last offset of last group, reset it to 0
                offset = 0;
                RepairGroup topGroup = rankingManager.getTopGroup();

                if (topGroup == null) {
                    // no more repair groups. break
                    break;
                }

                RandomForestClassifier classifier = new RandomForestClassifier(getCurrentContext(), dirtyTableSchema, NadeefConfiguration.getMLAttributes(), topGroup.getColumn(), 10);
//                J48Classifier classifier = new J48Classifier(getCurrentContext(), dirtyTableSchema, NadeefConfiguration.getMLAttributes(), topGroup.getColumn());
                String trainingSetFilePath = "examples/tax1KtrainingSet/training_instances_" + topGroup.getColumn().getColumnName() + ".arff";
                classifier.trainClassifier(trainingSetFilePath);

                // TODO one manually generated trainingInstance to start classifier.
//                Tuple tuple = DBConnectionHelper.getDatabaseTuple(sourceConnectionPool, dirtyTableName, dirtyTableSchema, 5);
//                TrainingInstance sampleInstance = new TrainingInstance(TrainingInstance.Label.YES, tuple, topGroup.getColumn().getColumnName(), tuple.getCell(topGroup.getColumn()).getValue().toString(), 1);
//                classifier.updateClassifier(sampleInstance);

                topGroup.populateFixByEntropy(classifier);

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
                    boolean classifierAnswer = getPredictionResult(classifier, newTrainingInstance);

                    if (isHit) {
                        // HIT :)) dirty cell correctly identified, now update database, reset the offset
                        offset = 0;

                        // increase hit count
                        hitCount++;

                        auditManager.applyFix(solution);

                        // add positive training instance
                        newTrainingInstance = new TrainingInstance(TrainingInstance.Label.YES, dirtyTuple, attribute, solutionValue.toString(), similarityScore);

                        // call ConsistencyManager to recompute violatios
                        Cell updatedCell = new Cell.Builder().tid(tupleID).column(new Column(dirtyTableName, attribute)).value(solutionValue).build();
                        // remove existing violations
                        ConsistencyManager.getInstance().removeViolations(updatedCell, getCurrentContext());
                        // find new violations
                        ConsistencyManager.getInstance().findNewViolations(updatedCell, getCurrentContext());

                        topGroup.populateFixByEntropy(classifier);
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
                    classifier.updateClassifier(newTrainingInstance);

                    userInteractionCount++;

                    // handle per attribute counters
                    interactionPerAttribute++;
                    if (isHit) {
                        hitPerAttribute++;
                        if (classifierAnswer) {
                            predictedHitPerAttribute++;
                        } else {
                            falsePredictedHITPerAttribute++;
                        }
                    } else {
                        if (classifierAnswer) {
                            falsePredictedNOTHitPerAttribute++;
                        } else {
                            predictedNOTHitPerAttribute++;
                        }
                    }
                }

                //TODO print machine learning accuracy metrics

                System.out.println(topGroup.getColumn().getColumnName() + " total hit: " + hitPerAttribute);
                System.out.println(topGroup.getColumn().getColumnName() + " total interaction: " + interactionPerAttribute);
                System.out.println(topGroup.getColumn().getColumnName() + " true positive: " + predictedHitPerAttribute);
                System.out.println(topGroup.getColumn().getColumnName() + " false positive: " + falsePredictedHITPerAttribute);
                System.out.println(topGroup.getColumn().getColumnName() + " true negative: " + predictedNOTHitPerAttribute);
                System.out.println(topGroup.getColumn().getColumnName() + " false negative: " + falsePredictedNOTHitPerAttribute);
            }
        } catch (Exception e) {
            tracer.error("Guided repair could NOT be completed due to SQL Expcetion: ", e);
            throw e;
        }

        long elapseTime = stopwatch.elapsed(TimeUnit.MILLISECONDS);

        PerfReport.appendMetric(PerfReport.Metric.RepairCallTime, elapseTime);
        PerfReport.appendMetric(PerfReport.Metric.UserInteractionHITCount, hitCount);
        PerfReport.appendMetric(PerfReport.Metric.UserInteractionCount, userInteractionCount);
        stopwatch.stop();
        return trainingInstances;
    }

    public boolean getPredictionResult(ClassifierBase classifier, TrainingInstance instance) throws NadeefClassifierException {
        ClassificationResult result = classifier.getPrediction(instance);
        TrainingInstance.Label topLabel = result.getTopLabel();

        return topLabel.equals(TrainingInstance.Label.YES) ? true : false;
    }

}
