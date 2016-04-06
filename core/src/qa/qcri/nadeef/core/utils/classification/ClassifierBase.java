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

import com.google.common.collect.Maps;
import qa.qcri.nadeef.core.datamodel.*;
import qa.qcri.nadeef.core.exceptions.NadeefClassifierException;
import qa.qcri.nadeef.core.exceptions.NadeefDatabaseException;
import qa.qcri.nadeef.core.pipeline.ExecutionContext;
import weka.core.Attribute;
import weka.core.FastVector;
import weka.core.Instance;
import weka.core.Instances;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by apacaci on 4/4/16.
 */
public abstract class ClassifierBase {

    private ExecutionContext context;
    private Schema databaseSchema;
    private List<String> permittedAttributes;
    private Map<String, List<String>> nominalValuesMap;

    protected final int numberOfAttributes;
    protected FastVector wekaAttributes;
    protected Instances instances;
    protected HashMap<Column, Integer> attributeIndex;

    public ClassifierBase(ExecutionContext executionContext, Schema databaseSchema, List<String> permittedAttributes) {
        this.context = executionContext;
        this.databaseSchema = databaseSchema;
        this.permittedAttributes = permittedAttributes;
        // all columns from database + new value + similarity + label
        this.numberOfAttributes = permittedAttributes.size() + 3;
        this.nominalValuesMap = Maps.newHashMap();

        this.wekaAttributes = new FastVector(this.numberOfAttributes);
        this.attributeIndex = Maps.newHashMap();
    }

    protected ExecutionContext getCurrentContext() {
        return context;
    }

    protected Schema getCurrentDatabaseSchema() {
        return databaseSchema;
    }

    protected List<String> getPermittedAttributes() {
        return permittedAttributes;
    }

    /**
     * Generates {@link Attribute} for both numeric and nominal values from database columns. Reads distinct values from database for categorical features
     *
     * @param columnName
     * @param attributeName if <code>null</code> or empty, then column name is used as the attribue name
     * @return
     * @throws NadeefDatabaseException
     */
    protected Attribute createAttributeFromSchema(String columnName, String attributeName) throws NadeefDatabaseException {
        // if attribute name is null or empty, then use column name
        attributeName = attributeName == null || attributeName.isEmpty() ? columnName : attributeName;
        Attribute attribute;
        if (nominalValuesMap.containsKey(columnName)) {
            List<String> values = nominalValuesMap.get(columnName);
            attribute = createAttribute(attributeName, values );
        } else {
            DataType columnType = databaseSchema.getType(columnName);
            if (columnType.equals(DataType.INTEGER) || columnType.equals(DataType.FLOAT) || columnType.equals(DataType.DOUBLE)) {
                // it is a numeric attribute
                attribute = createAttribute(attributeName, null);
            } else {
                // means that attribute is not numeric, so it should be nominal (assuming that we do not create attribute for arbitrary strings)
                List<String> values = ClassificationHelper.getDistinctValues(getCurrentContext().getConnectionPool().getSourceDBConfig(), databaseSchema.getTableName(), columnName);
                nominalValuesMap.put(columnName, values);
                attribute = createAttribute(attributeName, values);
            }
        }

        return attribute;
    }

    /**
     * Generates
     *
     * @param attributeName
     * @param values        list of values for categorical features. use <code>null</code> or empty list for numeric features
     * @return
     */
    protected Attribute createAttribute(String attributeName, List<String> values) {
        Attribute attribute;
        if (values == null || values.isEmpty()) {
            // it is a numeric attribute
            attribute = new Attribute(attributeName);
        } else {
            // means that attribute is not numeric, so it should be nominal (assuming that we do not create attribute for arbitrary strings)
            FastVector nominalVector = new FastVector();
            for (String value : values) {
                nominalVector.addElement(value);
            }
            attribute = new Attribute(attributeName, nominalVector);
        }
        return attribute;
    }

    /**
     * Update the existing classifier with new instance. For online models, it directly updates. For offline learning models, it re-generates the model with updated training set
     *
     * @param instance
     */
    public void updateClassifier(TrainingInstance instance) throws NadeefClassifierException {
        // transform training instance into real instance
        Instance wekaInstance = new Instance(numberOfAttributes);
        // add values from old tuple
        for (Cell cell : instance.getDirtyTuple().getCells()) {
            if (isPermitted(cell.getColumn())) {
                wekaInstance.setValue(attributeIndex.get(cell.getColumn()), cell.getValue().toString());
            }
        }

        // add new value
        wekaInstance.setValue(numberOfAttributes - 3, instance.getUpdatedCell().getValue().toString());
        // add similarity
        wekaInstance.setValue(numberOfAttributes - 2, instance.getSimilarityScore());
        // add class label
        wekaInstance.setValue(numberOfAttributes - 1, instance.getLabel().toString());

        updateClassifier(wekaInstance);
    }

    protected abstract void updateClassifier(Instance instance) throws NadeefClassifierException;

    /**
     * Get Prediction for a given instance based on current model
     *
     * @param instance
     */
    public void getPrediction(TrainingInstance instance) throws NadeefClassifierException {
        // transform training instance into real instance
        Instance wekaInstance = new Instance(numberOfAttributes);
        // add values from old tuple
        for (Cell cell : instance.getDirtyTuple().getCells()) {
            if (isPermitted(cell.getColumn())) {
                wekaInstance.setValue(attributeIndex.get(cell.getColumn()), cell.getValue().toString());
            }
        }

        // add new value
        wekaInstance.setValue(numberOfAttributes - 3, instance.getUpdatedCell().getValue().toString());
        // add similarity
        wekaInstance.setValue(numberOfAttributes - 2, instance.getSimilarityScore());

        double[] result = getPrediction(wekaInstance);
        // now convert this result into readable form
        ClassificationResult classificationResult = new ClassificationResult(result, wekaInstance.attribute(this.numberOfAttributes - 1));
    }

    protected abstract double[] getPrediction(Instance instance) throws NadeefClassifierException;

    /**
     * Checks whether given column is a feature for this model
     *
     * @param columnName
     * @return
     */
    protected boolean isPermitted(String columnName) {
        return this.permittedAttributes.contains(columnName.toLowerCase());
    }

    /**
     * Checks whether given column is a feature for this model
     *
     * @param column
     * @return
     */
    protected boolean isPermitted(Column column) {
        return isPermitted(column.getColumnName());
    }
}
