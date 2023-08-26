package com.hzw.fdc.engine.calculatedindicator.command;

import bsh.CallStack;
import bsh.Interpreter;

public class test {
    public static void invoke(Interpreter env, CallStack callstack )
    {
        String dir = ".";

        System.out.println("dddd");
    }

    public static void invoke(Interpreter env, CallStack callstack, Object... args )
    {
        String dir = ".";

        System.out.println("dddd:"+args);
    }
}
