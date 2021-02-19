package nl.appmodel;

import lombok.extern.slf4j.Slf4j;
import org.hibernate.Session;
import org.hibernate.cfg.Configuration;
import org.hibernate.cfg.Environment;
import java.util.Properties;
@Slf4j
public class LegacyUtil {
    public static Session session() {
        try {
            var properties = new Properties();
            properties.put(Environment.DRIVER, "com.mysql.cj.jdbc.Driver");
            //&AllowPublicKeyRetrieval=True
            properties.put(Environment.URL, "jdbc:mysql://localhost:3306/prosite?serverTimezone=UTC&useSSL=false");
            properties.put(Environment.USER, "root");
            properties.put(Environment.PASS, "Welkom01!");
            properties.put(Environment.DIALECT, "org.hibernate.dialect.MySQL8Dialect");
            properties.put(Environment.CURRENT_SESSION_CONTEXT_CLASS, "thread");
            properties.put(Environment.HBM2DDL_AUTO, "none");

            return new Configuration()
                    .setProperties(properties)
                    .addAnnotatedClass(Pro.class)
                    .buildSessionFactory().openSession();
        } catch (Throwable ex) {
            System.err.println("build SeesionFactory failed :" + ex);
            throw new ExceptionInInitializerError(ex);
        }
    }
}
