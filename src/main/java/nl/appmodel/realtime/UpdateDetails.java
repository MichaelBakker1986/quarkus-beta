package nl.appmodel.realtime;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.hibernate.Session;
import javax.inject.Inject;
@Slf4j
public class UpdateDetails {
    private final Notifier notifier = new Notifier();
    @Inject       Session  session;
    @SneakyThrows
    public static void main(String[] args) {
        var updateDetails = new UpdateDetails();
        HibernateUtil.run(updateDetails::updateEntities);
    }

    public void updateEntities(Session session) {
        this.session = session;
        //this.updateEntities();
    }
}
