package nl.appmodel.realtime;

public interface LogSetup {
    /*//@formatter:off
     Object[][] LEVELS = {
            { 
             Environment.class,Level.WARN
            }
    };
    //@formatter:on
    static void setupLog(Level base_level) {
        var hibernate  = java.util.logging.Logger.getLogger("org.hibernate");
        var jboss      = java.util.logging.Logger.getLogger("org.jboss");
        var rootLogger = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);

        rootLogger.setLevel(base_level);
        rootLogger.addAppender(createLoggerFor(new File("./log.txt")));

        var stdout = (ConsoleAppender<ILoggingEvent>) rootLogger.getAppender("console");
        var layout = new PatternLayout();
        layout.setPattern("%highlight(%d{ss} %-40(.\\(%F:%L\\)) %d{SSS}) %m%n");
        layout.setContext(stdout.getContext());
        stdout.setLayout(layout);

        layout.start();

        for (var log_configuration : LEVELS) {
            var o = (Level) log_configuration[1];
            if (o.isGreaterOrEqual(base_level)) {
                ((Logger) LoggerFactory.getLogger((Class<?>) log_configuration[0])).setLevel((Level) log_configuration[1]);
            }
        }
        hibernate.setLevel(java.util.logging.Level.WARNING);
        hibernate.setUseParentHandlers(false);
        jboss.setLevel(java.util.logging.Level.WARNING);
        jboss.setUseParentHandlers(false);
    }
    private static FileAppender<ILoggingEvent> createLoggerFor(File file) {
        Context              lc  = (Context) LoggerFactory.getILoggerFactory();
        PatternLayoutEncoder ple = new PatternLayoutEncoder();

        ple.setPattern("%date %level [%thread] %logger{10} [%file:%line] %msg%n");
        ple.setContext(lc);
        ple.start();
        FileAppender<ILoggingEvent> fileAppender = new FileAppender<ILoggingEvent>();
        fileAppender.setFile(file.toPath().toString());
        fileAppender.setEncoder(ple);
        fileAppender.setContext(lc);
        fileAppender.start();
        return fileAppender;
    }*/
}
