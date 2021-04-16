package nl.appmodel.realtime;

import lombok.extern.slf4j.Slf4j;
import lombok.val;
import nl.appmodel.realtime.model.NetworkHash;
import nl.appmodel.realtime.model.PornHub;
import nl.appmodel.realtime.model.YouPorn;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.cfg.Configuration;
import org.hibernate.cfg.Environment;
import java.util.Properties;
import java.util.function.Consumer;
@Slf4j
public class HibernateUtill {
    public static void run(Consumer<Session> run) {
        val session = HibernateUtill.getCurrentSession();
        session.getTransaction().begin();
        run.accept(session);
        session.getTransaction().commit();
        session.close();
    }
    public static Session getCurrentSession() {

        var properties = new Properties();
        properties.put(Environment.DRIVER, "com.mysql.cj.jdbc.Driver");
        properties.put(Environment.URL, "jdbc:mysql:///prosite?serverTimezone=UTC&allowMultiQueries=true&namedPipePath=\\\\.\\pipe\\MySQL");
        properties.put(Environment.USER, "server");
        properties.put(Environment.PASS, "Welkom05!");
        properties.put(Environment.FORMAT_SQL, "false");
        properties.put(Environment.DIALECT, "org.hibernate.dialect.MySQL8Dialect");
        properties.put(Environment.SHOW_SQL, "false");
        properties.put(Environment.CURRENT_SESSION_CONTEXT_CLASS, "thread");
        properties.put(Environment.HBM2DDL_AUTO, "none");
        properties.put(Environment.JPA_LOCK_TIMEOUT, 3600);

        return new Configuration()
                .addAnnotatedClass(PornHub.class)
                .addAnnotatedClass(NetworkHash.class)
                .addAnnotatedClass(YouPorn.class)
                .setProperties(properties)
                .buildSessionFactory().getCurrentSession();
    }
    public static SessionFactory em() {
        var properties = new Properties();
        properties.put(Environment.DRIVER, "com.mysql.cj.jdbc.Driver");
        properties.put(Environment.URL, "jdbc:mysql:///prosite?serverTimezone=UTC&allowMultiQueries=true&namedPipePath=\\\\.\\pipe\\MySQL");
        properties.put(Environment.USER, "server");
        properties.put(Environment.PASS, "Welkom05!");
        properties.put(Environment.FORMAT_SQL, "false");
        properties.put(Environment.DIALECT, "org.hibernate.dialect.MySQL8Dialect");
        properties.put(Environment.SHOW_SQL, "false");
        properties.put(Environment.CURRENT_SESSION_CONTEXT_CLASS, "thread");
        properties.put(Environment.HBM2DDL_AUTO, "none");
        properties.put(Environment.JPA_LOCK_TIMEOUT, 3600);

        return new Configuration()
                .addAnnotatedClass(PornHub.class)
                .addAnnotatedClass(NetworkHash.class)
                .addAnnotatedClass(YouPorn.class)
                .setProperties(properties)
                .buildSessionFactory();
    }
}
