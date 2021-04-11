package nl.appmodel.realtime;

import lombok.SneakyThrows;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.hibernate.Session;
import javax.inject.Inject;
@Slf4j
@ToString
public class DownloadedVideosJob {
    private final Notifier notifier = new Notifier();
    @Inject       Session  s;
    
    public void updateEntities(Session session) {
        this.s = session;
        //this.insertRowsToProTable();
    }
    @SneakyThrows
    public static void main(String[] args) {
        var job = new DownloadedVideosJob();
        HibernateUtill.run(job::updateEntities);
    }
}
