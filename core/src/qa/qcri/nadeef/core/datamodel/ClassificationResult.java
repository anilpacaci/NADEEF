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

import weka.core.Attribute;

import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;

/**
 * Class to represent Weka Classification Result
 * Created by apacaci on 4/5/16.
 */
public class ClassificationResult {

    private Map<Object, Double> classDistributions;
    private Object topClass;

    public ClassificationResult(double[] prediction, Attribute labelAttribute) {
        classDistributions = new HashMap<>();

        double maxProbability = 0;
        for(int i = 0 ; i < prediction.length ; i++) {
            double probability = prediction[i];
            String value = labelAttribute.value(i);
            classDistributions.put(value, probability);
            if(probability > maxProbability) {
                topClass = value;
                maxProbability = probability;
            }
        }
    }

    public Object getTopLabel() {
        return topClass;
    }

    public double getProbability(Object classLabel) {
        return classDistributions.get(classLabel);
    }

}
