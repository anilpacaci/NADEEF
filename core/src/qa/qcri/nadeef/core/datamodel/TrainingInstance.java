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

/**
 * Created by apacaci on 3/29/16.
 * <p>
 * Data representation for the Machine Learning component of the project. Based on the data representation of the Guided Data Repair by Ihab Ilyas
 */
public class TrainingInstance {
    private Label label;
    private Tuple dirtyTuple;
    private Cell updatedCell;
    private double similarityScore;

    public TrainingInstance(Label label, Tuple dirtyTuple, Cell updatedCell, double similarityScore) {
        this.label = label;
        this.dirtyTuple = dirtyTuple;
        this.updatedCell = updatedCell;
        this.similarityScore = similarityScore;
    }

    public Tuple getDirtyTuple() {
        return dirtyTuple;
    }

    public void setDirtyTuple(Tuple dirtyTuple) {
        this.dirtyTuple = dirtyTuple;
    }

    public Label getLabel() {
        return label;
    }

    public void setLabel(Label label) {
        this.label = label;
    }

    public Cell getUpdatedCell() {
        return updatedCell;
    }

    public void setUpdatedCell(Cell updatedCell) {
        this.updatedCell = updatedCell;
    }

    public double getSimilarityScore() {
        return similarityScore;
    }

    public void setSimilarityScore(double similarityScore) {
        this.similarityScore = similarityScore;
    }

    public enum Label {
        YES(0),
        NO(1);

        private final int value;

        private Label(int value) {
            this.value = value;
        }

        public int getValue() {
            return value;
        }

    }

}
