package nl.appmodel.realtime;

public interface LongCall {
    default long call() {
        try {
            return call2();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    long call2() throws Exception;
}
