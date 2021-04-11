package nl.appmodel.pornhub;

import io.quarkus.scheduler.Scheduled;
import lombok.AllArgsConstructor;
import lombok.SneakyThrows;
import lombok.ToString;
import lombok.extern.java.Log;
import lombok.val;
import nl.appmodel.PornHubHash;
import nl.appmodel.realtime.HibernateUtil;
import nl.appmodel.realtime.Update;
import org.eclipse.microprofile.context.ManagedExecutor;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.jdbc.Work;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.persistence.FlushModeType;
import javax.persistence.LockModeType;
import javax.transaction.Transactional;
import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.zip.CRC32;
import java.util.zip.ZipInputStream;
@Log
@ApplicationScoped
public class FullPornHubUpdates implements Update {
    private       int          changes       = 0;
    private final List<String> sqlStatements = new ArrayList<>();
    private final Set<Long>    seenIds       = new HashSet<>();
    private       long         skipped       = 0, examinedRecords = 0;
    private              URL             url;
    private static final char            separator = '|';
    private final        Map<Long, Long> hashes    = new ConcurrentHashMap<>();
    String URL   = "https://www.pornhub.com/files/pornhub.com-db.zip";
    int    total = 0;
    @Inject
    ManagedExecutor executor;
    Executor executor_used;
    @Inject SessionFactory fact;
    @Inject
    Session session;
    @SneakyThrows
    public static void main(String[] args) {
        var pornHubUpdates = new FullPornHubUpdates();
        pornHubUpdates.url = new URL("https://www.pornhub.com/files/pornhub.com-db.zip");
        //pornHubUpdates.URL           = "C:\\Users\\michael\\Downloads\\pornhub.com-db.zip";
        //pornHubUpdates.url           = new File(pornHubUpdates.URL).toURI().toURL();
        pornHubUpdates.executor_used = Executors.newCachedThreadPool();
        pornHubUpdates.fact          = HibernateUtil.em();
        pornHubUpdates.session       = HibernateUtil.getCurrentSession();
        pornHubUpdates.session.getTransaction().begin();
        pornHubUpdates.preflight();
    }
    public void cleanUp() {
        examinedRecords = 0;
        skipped         = 0;
        changes         = 0;
        seenIds.clear();
        hashes.clear();
        session.close();
        sqlStatements.clear();
    }
    private void resolveHashes(Consumer<PornHubHash> consumer) {
        executor_used.execute(() -> {
            session.setFlushMode(FlushModeType.AUTO);
     /*       NativeQuery nativeQuery = session.createNativeQuery("SELECT pornhub,crc FROM pornhub ORDER BY pornhub FOR UPDATE SKIP LOCKED", "REMAP");
            nativeQuery.setReadOnly(true);
            nativeQuery.stream().forEach(o -> {
                log.info("[{}]" + o.toString());
            });*/
            var query = session.createQuery("SELECT new nl.appmodel.PornHubHash(pornhub,crc) from PornHub order by pornhub",
                                            PornHubHash.class);
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
        sqlStatements.clear();
        if (executor_used == null) executor_used = executor;
        if (url == null) url = new URL(URL);
        resolveHashes(pornHubHash -> hashes.put(pornHubHash.getPornhub(), pornHubHash.getCrc()));
        this.pornhubVideos(-1L);
        cleanOptimisticRows();
        batchPersist();
        setUnavailableStatus();
        cleanUp();
    }
    private void cleanOptimisticRows() {

    }
    @Scheduled(cron = "0 09 02 * * ?", identity = "full-pornhub-videos-update")
    @Transactional
    @SneakyThrows
    public void preflight() {
        sqlStatements.clear();
        if (executor_used == null) executor_used = executor;
        if (url == null) url = new URL(URL);
        resolveHashes(pornHubHash -> hashes.put(pornHubHash.getPornhub(), pornHubHash.getCrc()));
        preflight(session, this::pornhubVideos, url);
        batchPersist();
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
    @SneakyThrows
    public void pornhubVideos(Long offset) {
        try (val bis = new BufferedInputStream(url.openStream());
             val zis = new ZipInputStream(bis)) {
            val ze = zis.getNextEntry();
            log.info("File: %s Size: %s Last Modified %s"
                             .formatted(ze.getName(), ze.getSize(), LocalDate.ofEpochDay(ze.getTime() / MILLS_IN_DAY)));
            try (val br = new BufferedReader(new InputStreamReader(zis, StandardCharsets.UTF_8))) {
                readSourceFile(separator, br, this::readPornhubSourceFileEntry);
            }
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
    private void readPornhubSourceFileEntry(String[] strings) {
        //TODO: CRC should be here.. the setup now is less error prone
        //var crc32 = new CRC32();
        //crc32.update(String.join("", strings).getBytes(StandardCharsets.UTF_8));

        examinedRecords++;
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
            addToBatch(pornhub, new PornhubRow(pornhub,
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
    }
    private void addToBatch(long pornhub, PornhubRow row) {
        if (hashes.containsKey(pornhub)) {
            if (row.$crc == hashes.get(pornhub)) {
                skipped++;
            } else {
                sqlStatements.add(row.crc_meta32().sql());
            }
            hashes.remove(pornhub);
        } else {
            sqlStatements.add(row.crc_meta32().sql());
            seenIds.add(pornhub);
        }
        if (examinedRecords % 10000 == 0) {
            log.info("Progress Examined:[" + examinedRecords + "] Changes:[" + changes + "] total:[" + total + "] skipped" + skipped);
        }
        if (sqlStatements.size() >= 10000) batchPersist();
    }
    /**
     * batch large request synchronous partitioning
     */
    @SneakyThrows
    private void batchPersist() {
        var rest = sqlStatements;
        while (!rest.isEmpty()) {
            val batch_size = Math.min(100000, rest.size());
            val subSet     = new ArrayList<>(rest.subList(0, batch_size));
            rest = rest.subList(batch_size, rest.size());
            persistBatch(subSet);
        }
        sqlStatements.clear();
    }
    /**
     * Multithreading persisting solution
     */
    @Transactional
    private void persistBatch(List<String> subSet) {
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
            total += subSet.size();
            val em      = fact.createEntityManager();
            val session = em.unwrap(Session.class);
            Work work = con -> {
                em.getTransaction().begin();
                try (val stmt = con.prepareStatement(block)) {
                    var matched = stmt.executeUpdate();
                    log.info("Matched [" + matched + "][" + (changes += stmt
                            .getUpdateCount()) + "][" + total + "] skipped" + skipped + "/" + examinedRecords);
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
    @AllArgsConstructor
    class Text {
        String story, err;
        public void print() {
            try {
                Matcher m = Pattern.compile("at line ([0-9]+)").matcher(this.err);   // the pattern to search for
                m.find();
                var lines = Integer.parseInt(m.group(1));
                val split = this.story.split("\n");
                log.info(split[Math.max(0, lines - 1)] + "\n" + split[lines] + '\n' + split[Math.min(split.length, lines + 1)]);
            } catch (Exception e) {
                log.warning(story);
                log.warning(err);
                log.warning("Error" + e);
            }
        }
    }
    record Text2(String story, String err) {
        public void print() {
            try {
                Matcher m = Pattern.compile("at line ([0-9]+)").matcher(this.err);   // the pattern to search for
                m.find();
                var lines = Integer.parseInt(m.group(1));
                val split = this.story.split("\n");
                log.info(split[lines - 1] + "\n" + split[lines - 1] + '\n' + split[lines + 1]);
            } catch (Exception e) {
                log.warning(story);
                log.warning(err);
                log.warning("Error" + e);
            }
        }
    }
}
