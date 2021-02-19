package nl.appmodel;

import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import java.util.logging.Logger;
public class HelloJob implements Job {
    private static final Logger LOG = Logger.getLogger(String.valueOf(HelloJob.class));
    public HelloJob() {
    }
    public void execute(JobExecutionContext context)
    throws JobExecutionException {
        System.err.println("Hello!  HelloJob is executing.");
        LOG.info("TEST");
    }
}
