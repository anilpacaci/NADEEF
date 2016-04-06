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

import javax.swing.*;
import java.util.DoubleSummaryStatistics;
import java.util.Scanner;

/**
 * Created by apacaci on 4/4/16.
 */
public class Test {

    public static void main(String[] argv) {

        while (true) {

            System.out.println("Type 0 to terminate the program");
            System.out.println("Type 1 to calculate square root of a number");

            // Scanner is required to read from any stream, System.in refers to the command line
            Scanner scanner = new Scanner(System.in);
            String commandLine = scanner.next();
            int option = Integer.parseInt(commandLine);

            if(option == 0) {
                // 0 means exit
                break;
            }

            else if(option == 1) {
                // implement Question 1
                String numberString = JOptionPane.showInputDialog(null, "Enter a number to calculate its square root");
                Double number = Double.parseDouble(numberString);

                // according to flow chart, error is always 0.01
                Double error = 0.01;

                SquareRoot squareRoot = new SquareRoot();
                squareRoot.set(number, error);

                double result = squareRoot.calculateSquareRoot();

                JOptionPane.showMessageDialog(null, "Approximated square root of " + number + " = " + result, "Result", JOptionPane.INFORMATION_MESSAGE);
            }

        }
    }

}
