/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.core.util;

/**
 * Tracer is used for debugging / profiling / diagnoising purpose.
 * TODO: adds support for log4j.
 */
public class Tracer {
    private Class classType;

    //<editor-fold desc="Tracer creation">
    private Tracer() {}
    private Tracer(Class classType) {
        this.classType = classType;
    }

    public static Tracer getTracer(Class classType) {
        return new Tracer(classType);
    }
    //</editor-fold>

    //<editor-fold desc="Public methods">
    public void info(String msg) {
        System.out.println("In " + classType.getName() + " : " + msg);
    }

    public void err(String msg) {
        System.err.println("In " + classType.getName() + " : " + msg);
    }
    //</editor-fold>
}
