package nl.appmodel.xvideos;

import lombok.SneakyThrows;
import lombok.extern.java.Log;
import lombok.val;
import nl.appmodel.realtime.HibernateUtill;
import nl.appmodel.realtime.LongCall;
import nl.appmodel.realtime.SCVContext;
import nl.appmodel.realtime.Update;
import org.hibernate.Session;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.transaction.Transactional;
import java.awt.TrayIcon.MessageType;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.function.Consumer;
@Log
@ApplicationScoped
public class XVideoDeletionsALL implements Update {
    private static final String zip_url = "https://webmaster-tools.xvideos.com/xvideos.com-deleted-full.csv.zip";
    @Inject
    Session session;
    @SneakyThrows
    public static void main(String[] args) {
        var updates = new XVideoDeletionsALL();
        updates.session = HibernateUtill.getCurrentSession();
        updates.session.getTransaction().begin();
        var ctx          = new SCVContext(new URL(zip_url));
        var lineConsumer = updates.lineConsumer(ctx);
        var reader       = updates.readSourceFile('|', lineConsumer);
        var zipCall      = updates.readZip(ctx, reader);
        updates.preflight(updates.session, ctx, updates.prepare(ctx, zipCall));
        updates.session.getTransaction().commit();
        updates.session.close();
    }
    /*@Scheduled(cron = "0 55 03 * * ?", identity = "deletions-all-xvideos-videos")*/
    @Transactional
    @SneakyThrows
    public void preflight() {
        var ctx          = new SCVContext(new URL(zip_url));
        var lineConsumer = this.lineConsumer(ctx);
        var reader       = readSourceFile('|', lineConsumer);
        var zipCall      = readZip(ctx, reader);
        preflight(session, ctx, this.prepare(ctx, zipCall));
    }
    public Runnable prepare(SCVContext ctx, LongCall zipCall) {
        return () -> {
            try {
                ctx.reset();
                long totalLength = zipCall.call();
                batchPersist(ctx);
                notifier.displayTray("Success - " + getClass().getSimpleName() + " - delete",
                                     "deleted [" + ctx.getChanges() + "] offset [" + 0 + "] total [" + totalLength + "]",
                                     MessageType.INFO);
            } catch (Exception e) {
                e.printStackTrace();
                log.severe("ERROR" + e.getMessage());
                notifier.displayTray("Fail - " + getClass().getSimpleName(), e.getMessage(), MessageType.ERROR);
            } finally {
                ctx.reset();
            }
        };
    }
    @SneakyThrows
    private Consumer<String[]> lineConsumer(SCVContext ctx) {
        return (strings) -> {
            val url     = escape(strings[1]);
            var xvideos = url.split("/")[3].replaceAll("[^0-9]", "");
            ctx.add(xvideos);
        };
    }
    @SneakyThrows
    private void batchPersist(SCVContext ctx) {
        if (ctx.isEmpty()) return;
        val new_sqlStatements = new ArrayList<>(ctx.getSqlStatements());
        val asSet             = new HashSet<>(new_sqlStatements);
        ctx.getSqlStatements().clear();
        while (!asSet.isEmpty()) {
            var allStatements = new ArrayList<>(asSet);
            var subset        = new ArrayList<>(allStatements.subList(0, Math.min(allStatements.size(), 100000)));
            asSet.removeAll(subset);
            if (!subset.isEmpty()) {
                var sql = """
                          UPDATE prosite.xvideos 
                          SET
                          updated = DEFAULT,
                          flag=flag | 256
                          WHERE xvideos IN (%s)
                          """.formatted(String.join(",", subset));
                ctx.addChanges(session.createNativeQuery(sql)
                                      .executeUpdate());
            }
        }
    }
}
