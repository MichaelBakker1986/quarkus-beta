package nl.appmodel;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.quartz.JobDetail;
import org.quartz.SchedulerFactory;
import java.util.logging.Logger;
import static org.quartz.JobBuilder.newJob;
import static org.quartz.SimpleScheduleBuilder.simpleSchedule;
import static org.quartz.TriggerBuilder.newTrigger;
@Slf4j
@ToString
@Getter
@AllArgsConstructor
public class Scheduler {
    private static final Logger LOG = Logger.getLogger(String.valueOf(Scheduler.class));
    @SneakyThrows public void start() {
        /*
        Jobs to schedule
        1) update feed from pornhub
        2) delete feed from pornhub
        3) most_used update queries
        4) 
         */

        SchedulerFactory schedFact = new org.quartz.impl.StdSchedulerFactory();
        var              sched     = schedFact.getScheduler();
        sched.start();
        // define the job and tie it to our HelloJob class
        JobDetail job = newJob(HelloJob.class)
                .withIdentity("myJob", "group1") // name "myJob", group "group1"
                .build();

        // Trigger the job to run now, and then every 40 seconds
        var trigger = newTrigger()
                .withIdentity("myTrigger", "group1")
                .startNow()
                .withSchedule(simpleSchedule()
                                      .withIntervalInSeconds(1)
                                      .repeatForever())
                .build();

        // Tell quartz to schedule the job using our trigger
        sched.scheduleJob(job, trigger);
    }
    public static void main(String[] args) {
        new Scheduler().start();
    }
}
