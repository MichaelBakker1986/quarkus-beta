package nl.appmodel.pornhub;

import io.quarkus.scheduler.Scheduled;
import lombok.AllArgsConstructor;
import lombok.SneakyThrows;
import lombok.ToString;
import lombok.extern.java.Log;
import lombok.val;
import nl.appmodel.realtime.HibernateUtill;
import nl.appmodel.realtime.SCVContext;
import nl.appmodel.realtime.Update;
import nl.appmodel.realtime.model.NetworkHash;
import org.eclipse.microprofile.context.ManagedExecutor;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.jdbc.Work;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.persistence.FlushModeType;
import javax.persistence.LockModeType;
import javax.transaction.Transactional;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.zip.CRC32;
@Log
@ApplicationScoped
public class FullPornHubUpdates implements Update {
    private final Set<Long>       seenIds = new HashSet<>();
    private final Map<Long, Long> hashes  = new ConcurrentHashMap<>();
    static        String          URL     = "https://www.pornhub.com/files/pornhub.com-db.zip";
    @Inject
    ManagedExecutor executor;
    Executor executor_used;
    @Inject SessionFactory fact;
    @Inject
    Session session;
    @SneakyThrows
    public static void main(String[] args) {
        var updates = new FullPornHubUpdates();
        URL = "https://www.pornhub.com/files/pornhub.com-db.zip";
        // URL           = "C:\\Users\\michael\\Downloads\\pornhub.com-db.zip";
        // url           = new File(updates.URL).toURI().toURL();
        updates.executor_used = Executors.newCachedThreadPool();
        updates.fact          = HibernateUtill.em();
        updates.session       = HibernateUtill.getCurrentSession();
        updates.session.getTransaction().begin();
        updates.preflight();
    }
    public void cleanUp() {
        seenIds.clear();
        hashes.clear();
        session.close();
    }
    private void resolveHashes(Consumer<NetworkHash> consumer) {
        executor_used.execute(() -> {
            session.setFlushMode(FlushModeType.AUTO);
     /*       NativeQuery nativeQuery = session.createNativeQuery("SELECT pornhub,crc FROM pornhub ORDER BY pornhub FOR UPDATE SKIP LOCKED", "REMAP");
            nativeQuery.setReadOnly(true);
            nativeQuery.stream().forEach(o -> {
                log.info("[{}]" + o.toString());
            });*/
            var query = session.createQuery("SELECT new nl.appmodel.realtime.model.NetworkHash(pornhub,crc) from PornHub order by pornhub",
                                            NetworkHash.class);
            query.setLockMode(LockModeType.NONE);
            query.setReadOnly(true);
            try (val stream = query
                    .stream()) {
                stream.forEach(consumer);
            }
        });
    }
    @SneakyThrows
    public void preflight_skip_dl() {
        if (executor_used == null) executor_used = executor;
        resolveHashes(pornHubHash -> hashes.put(pornHubHash.getPornhub(), pornHubHash.getCrc()));
        var ctx          = new SCVContext(new URL(URL));
        var lineConsumer = this.lineConsumer(ctx);
        this.readZip(ctx, readSourceFile('|', lineConsumer));
        cleanOptimisticRows();
        batchPersist(ctx).run();
        setUnavailableStatus();
        cleanUp();
    }
    private void cleanOptimisticRows() {
    }
    @Scheduled(cron = "0 09 02 * * ?", identity = "full-pornhub-videos-update")
    @Transactional
    @SneakyThrows
    public void preflight() {
        if (executor_used == null) executor_used = executor;
        resolveHashes(pornHubHash -> hashes.put(pornHubHash.getPornhub(), pornHubHash.getCrc()));
        var ctx             = new SCVContext(new URL(URL));
        var lineConsumer    = this.lineConsumer(ctx);
        var resourceHandler = this.readSourceFile('|', lineConsumer);
        preflight(session, ctx, () -> this.readZip(ctx, resourceHandler));
        batchPersist(ctx).run();
        setUnavailableStatus();
        cleanUp();
    }
    public void nonpornhubVideos(Long offset) {

    }
    private void setUnavailableStatus() {
        val keys = new HashSet<>(hashes.keySet());
        keys.removeAll(seenIds);
        if (!keys.isEmpty()) {
            val collect = keys.stream().map(aLong -> String.valueOf(aLong)).collect(Collectors.joining(","));
            val nativeQuery = session.createNativeQuery("""
                                                        UPDATE prosite.pornhub ph 
                                                        FORCE INDEX (flag_pro)
                                                        NATURAL JOIN prosite.valid_flag
                                                        SET ph.flag    =ph.flag | 1024,
                                                            ph.updated =DEFAULT 
                                                        WHERE pornhub IN (%s)
                                                        """.formatted(collect));
            val changes = nativeQuery.executeUpdate();
            log.info("Unavailable records:" + changes);
        }
    }
    public long pornhubIdFromThumb(String in) {
        val split = in.substring(10, 64).split("/");
        if (split.length < 4) return -in.length();
        return sqlNumber(split[4]);
    }
    @ToString
    @AllArgsConstructor
    class PornhubRow {
        long   pornhub;
        String code, header, tag, cat, actor;
        long duration, views, up, down;
        String preview_d;
        long   $crc;
        long   $crc_meta;
        Dim    $dim;
        private String keyId(Dim dim) {
            return escape(dim.getSrc().split("/")[4]);
        }
        public String sql() {
            return "(" + up + "," + down + "," + views + "," + duration + ",\"" + cat + "\",\"" + tag + "\",\"" + actor + "\",\"" + header + "\",\"" + preview_d + "\"," + $dim
                    .getW() + "," + $dim.getH() + "," + pornhub + ",\"" + keyId($dim) + "\"," + $crc_meta + "," + $crc + ")";
        }
        public PornhubRow crc_meta32() {
            var crc32 = new CRC32();
            //CRC32(CONCAT_WS('',tag, cat, actor, header, thumbs, w, h, duration))
            crc32.update((tag + cat + actor + "" + header + "" + preview_d + "" + $dim.getW() + "" + $dim.getH() + "" + duration)
                                 .getBytes(StandardCharsets.UTF_8));
            this.$crc_meta = crc32.getValue();
            return this;
        }
        public PornhubRow crc32() {
            $dim = dims(this.code);
            var crc32 = new CRC32();
            //CRC32(CONCAT_WS('',tag, cat, views, up, down, IF(flag & 256 = 0, 0, 1)))
            crc32.update((tag + cat + views + "" + up + "" + down + "" + 0).getBytes(StandardCharsets.UTF_8));
            this.$crc = crc32.getValue();
            return this;
        }
    }
    @SneakyThrows
    private Consumer<String[]> lineConsumer(SCVContext ctx) {
        return (strings) -> {
            //TODO: CRC should be here.. the setup now is less error prone
            //var crc32 = new CRC32();
            //crc32.update(String.join("", strings).getBytes(StandardCharsets.UTF_8));
            ctx.addExamined();
            val pornhub = pornhubIdFromThumb(strings[1]);
            if (strings.length < 13 ||
                Arrays.stream(strings).anyMatch(s -> s == null) ||
                pornhubIdFromThumb(strings[11]) != pornhub ||
                pornhub < 0
            ) {
                log.warning("Invalid record, skipping [" + String.join(" ", strings) + "]");
                return;
            }
            try {
                addToBatch(ctx, pornhub, new PornhubRow(pornhub,
                                                        strings[0],
                                                        escapeStrict(strings[3]),
                                                        escapeStrict(strings[4]),
                                                        escapeStrict(strings[5]),
                                                        escapeStrict(strings[6]),
                                                        Math.min(sqlNumber(strings[7]), 65535),
                                                        INT(sqlNumber(strings[8])),
                                                        INT(sqlNumber(strings[9])),
                                                        INT(sqlNumber(strings[10])),
                                                        CONCAT(escape(strings[11]), escape(strings[12]))).crc32()
                          );
            } catch (Exception e) {
                log.warning("Invalid record " + String.join(" ", strings));
            }
        };
    }
    private void addToBatch(SCVContext ctx, long pornhub, PornhubRow row) {
        if (hashes.containsKey(pornhub)) {
            if (row.$crc == hashes.get(pornhub)) {
                ctx.addSkipped();
            } else {
                ctx.add(row.crc_meta32().sql());
            }
            hashes.remove(pornhub);
        } else {
            ctx.add(row.crc_meta32().sql());
            seenIds.add(pornhub);
        }
        if (ctx.getExamined() % 10000 == 0) {
            log.info("Progress Examined:[" + ctx.getExamined() + "] Changes:[" + ctx
                    .getChanges() + "] total:[" + ctx.getTotal() + "] skipped" + ctx.getStats().skipped);
        }
        if (ctx.size() >= 10000) batchPersist(ctx).run();
    }
    /**
     * batch large request synchronous partitioning
     */
    @SneakyThrows
    private Runnable batchPersist(SCVContext ctx) {
        return () -> {
            var rest = ctx.getSqlStatements();
            while (!rest.isEmpty()) {
                val batch_size = Math.min(100000, rest.size());
                val subSet     = new ArrayList<>(rest.subList(0, batch_size));
                rest = rest.subList(batch_size, rest.size());
                persistBatch(ctx, subSet);
            }
            ctx.clear();
        };
    }
    /**
     * Multithreading persisting solution
     */
    @Transactional
    private void persistBatch(SCVContext ctx, List<String> subSet) {

        val block = """
                    INSERT INTO prosite.pornhub (up,down,views,duration,cat,tag,actor,header,preview_d,w,h,pornhub,keyid,$crc_meta,$crc)
                    SELECT * FROM pornhub AS new 
                    ON DUPLICATE KEY UPDATE 
                    up    =new.up,
                    down  =new.down,
                    views =new.views, 
                    duration=new.duration, 
                    cat     =IF(new.$crc_meta = prosite.pornhub.crc_meta, prosite.pornhub.cat, new.cat),
                    actor   =IF(new.$crc_meta = prosite.pornhub.crc_meta, prosite.pornhub.actor, new.actor),  
                    tag     =IF(new.$crc_meta = prosite.pornhub.crc_meta, prosite.pornhub.tag, new.tag), 
                    header=new.header, 
                    preview_d=IF(new.$crc_meta = prosite.pornhub.crc_meta, prosite.pornhub.preview_d, new.preview_d),
                    $crc_meta=IF(new.$crc_meta = prosite.pornhub.crc_meta, prosite.pornhub.$crc_meta, new.$crc_meta), 
                    $crc     =IF(new.$crc      = prosite.pornhub.crc,prosite.pornhub.$crc,new.$crc),
                    w      =new.w, 
                    h      =new.h, 
                    updated=DEFAULT, 
                    flag   = prosite.pornhub.flag & ~(
                                                IF(new.$crc_meta = prosite.pornhub.crc_meta, 0, 0b010) |
                                                IF(CRC32(CONCAT_WS('',new.views, new.up , new.down, 0))    = prosite.pornhub.crc_stats,0, 0b100)
                                                )
                    """.replace("SELECT * FROM pornhub AS new", "VALUES " + String.join(",\n", subSet) + " AS new ");
        //what i want to say to the status, meta done, stats done, (if downloaded or not stay that way) , if error/deleted or not stay that way.
        executor_used.execute(() -> {
            ctx.addTotal(subSet.size());
            val em      = fact.createEntityManager();
            val session = em.unwrap(Session.class);
            Work work = con -> {
                em.getTransaction().begin();
                try (val stmt = con.prepareStatement(block)) {
                    var matched = stmt.executeUpdate();
                    log.info("Matched [" + matched + "][" + (ctx.addChanges(
                            stmt.getUpdateCount())) + "][" + ctx.getTotal() + "] skipped" + ctx.getStats().skipped + "/" + ctx
                                     .getExamined());
                    em.getTransaction().commit();
                } catch (Exception e) {
                    new Text(block, e.getMessage()).print();
                    em.getTransaction().rollback();
                } finally {
                    em.close();
                }
            };
            session.doWork(work);
        });
    }
}
