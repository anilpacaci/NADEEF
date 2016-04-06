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

package qa.qcri.nadeef.test.classifier;

/**
 * Created by apacaci on 4/4/16.
 */
public class SquareRoot {

    /**
     * Private class members
     */
    private double number;
    private double error;

    /**
     * Class method to initialize error and numbers
     * @param number
     * @param error
     */
    public void set(double number, double error) {
        this.error = error;
        this.number = number;
    }

    /**
     * Class member to calculate the square root
     * @return Square root of the number previously set, using specified error (-1 if square root cannot be computed)
     */
    public double calculateSquareRoot() {

        // X in the flow chart
        double temp = 1;
        // S in the flow chart
        double result = -1;

        if(number < 0) {
            // no sqrt for negative numbers
            return -1;
        }

        // number is positive, compute sqrt
        do {
            result = (temp + (this.number / temp)) / 2;
            temp = result;
        } while(Math.abs(this.number - result * result) > this.error);

        return result;
    }
}
