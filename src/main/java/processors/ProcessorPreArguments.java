package processors;

public class ProcessorPreArguments {

    private final boolean thisProcessor;
    private final Object[] arguments;

    public ProcessorPreArguments(boolean thisProcessor, Object... arguments) {
        this.thisProcessor = thisProcessor;
        this.arguments = arguments;
    }

    public ProcessorPreArguments(boolean thisProcessor) {
        this.thisProcessor = thisProcessor;
        arguments = new Object[]{};
    }

    public boolean isThisProcessor() {
        return thisProcessor;
    }

    public Object[] getArguments() {
        return arguments;
    }

}
