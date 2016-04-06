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
public class J48Classifier extends ClassifierBase {

    private J48 classifier;

    public J48Classifier(ExecutionContext executionContext, Schema databaseSchema, List<String> permittedAttributes, Column newValueColumn) throws NadeefDatabaseException {
        super(executionContext, databaseSchema, permittedAttributes);

        // now we need to initalize Weka Feature Vector
        int attributeIndex = 0;
        for(Column column : databaseSchema.getColumns()) {
            Attribute attribute = createAttributeFromSchema(column.getColumnName(), null);
            this.wekaAttributes.addElement(attribute);
            this.attributeIndex.put(column, attributeIndex);
            attributeIndex++;
        }
        // new_value
        Attribute attribute = createAttributeFromSchema(newValueColumn.getColumnName(), "new_value");
        this.wekaAttributes.addElement(attribute);
        //similarity score
        attribute = createAttribute("similarity_score", null);
        this.wekaAttributes.addElement(attribute);
        // class label
        attribute = createAttribute("class_label", Arrays.asList("yes", "no"));
        this.wekaAttributes.addElement(attribute);
        // set class label
        instances = new Instances(databaseSchema.getTableName(), this.wekaAttributes, 0);
        instances.setClassIndex(this.numberOfAttributes - 1);

        // initialize the model
        J48 classifier = new J48();
    }

    protected void updateClassifier(Instance instance) throws NadeefClassifierException {
        instances.add(instance);

        try {
            classifier.buildClassifier(instances);
        } catch (Exception e) {
            throw new NadeefClassifierException("J48 cannot be built with new instance", e);
        }
    }

    @Override
    protected double[] getPrediction(Instance instance) throws NadeefClassifierException {
        try {
            return classifier.distributionForInstance(instance);
        } catch (Exception e) {
            throw new NadeefClassifierException("J48 cannot classifiy the instance", e);
        }
    }


}
