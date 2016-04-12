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

package qa.qcri.nadeef.core.utils.classification;

import qa.qcri.nadeef.core.datamodel.Column;
import qa.qcri.nadeef.core.datamodel.Schema;
import qa.qcri.nadeef.core.exceptions.NadeefClassifierException;
import qa.qcri.nadeef.core.exceptions.NadeefDatabaseException;
import qa.qcri.nadeef.core.pipeline.ExecutionContext;
import weka.classifiers.meta.Bagging;
import weka.classifiers.trees.J48;
import weka.classifiers.trees.RandomForest;
import weka.core.Attribute;
import weka.core.Instance;
import weka.core.Instances;

import java.util.Arrays;
import java.util.List;

/**
 * Created by apacaci on 4/5/16.
 */
public class RandomForestClassifier extends ClassifierBase {

    private int forestSize;

    public RandomForestClassifier(ExecutionContext executionContext, Schema tableSchema, List<String> permittedAttributes, Column newValueColumn, int forestSize) throws NadeefDatabaseException {
        super(executionContext, tableSchema, permittedAttributes, newValueColumn);
        this.forestSize = forestSize;

        // initialize the model
        this.classifier = new RandomForest();
        ((RandomForest)this.classifier).setNumTrees(forestSize);
    }

    protected void updateClassifier(Instance instance) throws NadeefClassifierException {
        instances.add(instance);
        instance.setDataset(instances);

        try {
            classifier.buildClassifier(instances);
        } catch (Exception e) {
            throw new NadeefClassifierException("RandomForest cannot be built with new instance", e);
        }
    }

    @Override
    protected double[] getPrediction(Instance instance) throws NadeefClassifierException {
        try {
            return classifier.distributionForInstance(instance);
        } catch (Exception e) {
            throw new NadeefClassifierException("RandomForest cannot classifiy the instance", e);
        }
    }


}
