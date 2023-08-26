package com.hzw.fdc.engine.calculatedindicator;

import javax.tools.Diagnostic;
import javax.tools.JavaFileObject;
import java.util.List;

public class CompileTestException extends Exception{
    private List<Diagnostic<? extends JavaFileObject>> list;

    public CompileTestException(List<Diagnostic<? extends JavaFileObject>> list) {
        this.list = list;
    }

    public List<Diagnostic<? extends JavaFileObject>> getList() {
        return list;
    }
}
