package nl.appmodel.realtime;

import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.hibernate.Session;
import org.hibernate.cfg.Configuration;
import org.hibernate.cfg.Environment;
import java.util.Properties;
import java.util.function.Consumer;
@Slf4j
public class HibernateUtil {
    public static void run(Consumer<Session> run) {
        val session = HibernateUtil.getCurrentSession();
        session.getTransaction().begin();
        run.accept(session);
        session.getTransaction().commit();
        session.close();
    }
    public static Session getCurrentSession() {

        var properties = new Properties();
        properties.put(Environment.DRIVER, "com.mysql.cj.jdbc.Driver");
        properties.put(Environment.URL, "jdbc:mysql:///prosite?serverTimezone=UTC&namedPipePath=\\\\.\\pipe\\MySQL");
        properties.put(Environment.USER, "server");
        properties.put(Environment.PASS, "Welkom05!");
        properties.put(Environment.FORMAT_SQL, "false");
        properties.put(Environment.DIALECT, "org.hibernate.dialect.MySQL8Dialect");
        properties.put(Environment.SHOW_SQL, "true");
        properties.put(Environment.CURRENT_SESSION_CONTEXT_CLASS, "thread");
        properties.put(Environment.HBM2DDL_AUTO, "none");
        properties.put(Environment.JPA_LOCK_TIMEOUT, 3600);

        return new Configuration()
                .setProperties(properties)
                .buildSessionFactory().getCurrentSession();
    }
}
