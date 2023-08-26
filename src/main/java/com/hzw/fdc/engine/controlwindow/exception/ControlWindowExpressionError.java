package com.hzw.fdc.engine.controlwindow.exception;

public class ControlWindowExpressionError extends Exception{

    public ControlWindowExpressionError() {
    }

    public ControlWindowExpressionError(String message) {
        super(message);
    }

    public ControlWindowExpressionError(String message, Throwable cause) {
        super(message, cause);
    }

    public static class UnDefinedExpressionError extends ControlWindowExpressionError{

    }

    public static class RSNStepBasedControlWindowExpressionError extends ControlWindowExpressionError{
        public RSNStepBasedControlWindowExpressionError() {
        }

        public RSNStepBasedControlWindowExpressionError(String message) {
            super(message);
        }

        public RSNStepBasedControlWindowExpressionError(String message, Throwable cause) {
            super(message, cause);
        }
    }
    public static class StepBasedControlWindowExpressionError extends ControlWindowExpressionError{
        public StepBasedControlWindowExpressionError() {
        }

        public StepBasedControlWindowExpressionError(String message) {
            super(message);
        }

        public StepBasedControlWindowExpressionError(String message, Throwable cause) {
            super(message, cause);
        }
    }
    public static class TimeBasedControlWindowExpressionError extends ControlWindowExpressionError{
        public TimeBasedControlWindowExpressionError() {
        }

        public TimeBasedControlWindowExpressionError(String message) {
            super(message);
        }

        public TimeBasedControlWindowExpressionError(String message, Throwable cause) {
            super(message, cause);
        }
    }

    public static class LogisticControlWindowExpressionError extends ControlWindowExpressionError{
        public LogisticControlWindowExpressionError() {
        }

        public LogisticControlWindowExpressionError(String message) {
            super(message);
        }

        public LogisticControlWindowExpressionError(String message, Throwable cause) {
            super(message, cause);
        }
    }

    public static class CycleControlWindowExpressionError extends ControlWindowExpressionError{
        public CycleControlWindowExpressionError() {
        }

        public CycleControlWindowExpressionError(String message) {
            super(message);
        }

        public CycleControlWindowExpressionError(String message, Throwable cause) {
            super(message, cause);
        }
    }

}
