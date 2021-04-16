package nl.appmodel.realtime;

import io.quarkus.scheduler.Scheduled;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import nl.appmodel.realtime.model.Network;
import org.hibernate.Session;
import javax.inject.Inject;
import javax.transaction.Transactional;
import java.awt.TrayIcon.MessageType;
@Slf4j
public class UpdateTags {
    private final Notifier notifier = new Notifier();
    @Inject       Session  session;
    /**
     * ADD/CONNECT ENTITY INTO PRO table
     */
    @SuppressWarnings("SqlResolve")
    @Transactional
    @Scheduled(cron = "0 05 22 * * ?", identity = "find-new-entries-from-networks")
    public void insertRowsToProTable() {
        for (Network network : Network.values()) {

            val changes = session.createNativeQuery("""
                                                    INSERT INTO pro ( header )  
                                                    SELECT NULL
                                                    FROM %s n 
                                                    NATURAL JOIN valid_flag_new_entries 
                                                    WHERE n.pro IS NULL;
                                                                                                        
                                                    /*use a join to get the incremental ID...*/
                                                    WITH virtual_pro AS
                                                    (
                                                    SELECT 
                                                     %s,
                                                     LAST_INSERT_ID() + ROW_NUMBER() OVER (ORDER BY pro) AS pro
                                                     FROM %s
                                                     NATURAL JOIN valid_flag_new_entries 
                                                     WHERE pro IS NULL
                                                    )
                                                    UPDATE pro p
                                                     JOIN virtual_pro USING (pro) JOIN %s n USING (%s) 
                                                    SET n.pro = p.pro    ,
                                                         p.views = n.views,
                                                         p.header = n.header,
                                                         p.embed = n.code,
                                                         p.w = n.w,
                                                         p.h = n.h,
                                                         p.up = n.up,
                                                         p.down = n.down,
                                                         p.duration = n.duration,
                                                         p.updated = DEFAULT,
                                                         n.updated = DEFAULT
                                                     WHERE n.pro IS NULL; 
                                                    """.formatted(network.tableName(), network.tableName(), network.tableName(),
                                                                  network.tableName(), network.tableName())
                                                   )
                                 .executeUpdate();
            log.info("Feedback: [{}] in timestamp:[{}]", changes);
        }
        log.info("Done with timestamp:[{}]", 1);
        notifier.displayTray("Updating pro", "Updating pro", MessageType.INFO);
    }
    /**
     * CONNECT ENTRIES WITH TAGS
     */
    @SuppressWarnings("SqlResolve")
    @Transactional
    @Scheduled(cron = "0 10 22 * * ?", identity = "connect-new-entries-tags")
    public void connect() {
        var updates = session.createNativeQuery("""
                                                 /*
                                                 Synchronizing script new PRO entities into tags
                                                 and pro_tags combinations
                                                */
                                                /*#Derrive tags from outstanding changes in meta_changes..*/
                                                /*#step 1, get the tags, display and index*/
                                                INSERT IGNORE INTO new_tag (pro,idx,display) 
                                                WITH top AS (SELECT pro,tag_count,tag_set FROM meta_changes) ,
                                                topxtagset AS (SELECT pro,SUBSTRING_INDEX(SUBSTRING_INDEX(tag_set, ',', t), ',', -1) AS display FROM tag_numbers JOIN top ON tag_count >= t ),
                                                topxtag AS (SELECT pro,
                                                REGEXP_REPLACE(
                                                REPLACE(
                                                REPLACE(
                                                REPLACE(
                                                REPLACE(
                                                REPLACE(
                                                REPLACE(
                                                REPLACE(
                                                REPLACE(
                                                REPLACE(
                                                REPLACE(
                                                REPLACE(
                                                REPLACE(
                                                REPLACE(
                                                REPLACE(
                                                REPLACE(
                                                REPLACE(
                                                REGEXP_REPLACE(
                                                REGEXP_REPLACE(
                                                REGEXP_REPLACE(
                                                REGEXP_REPLACE(
                                                REGEXP_REPLACE(
                                                REGEXP_REPLACE(
                                                UPPER(display)
                                                ,'[ÒÓÔÕÖØ]','O')
                                                ,'[ÙÚÛÜ]','U')
                                                ,'[ÌÍÎÏ¡Ÿİ¥I]','I')
                                                ,'[ÈÉÊË£]','E')
                                                ,'[ÀÁÂÃÄÅÆ©@Ą]','A')
                                                ,'[^A-Z0-9]','')
                                                ,'Y','I')
                                                ,'Z','S')
                                                ,'W','V')
                                                ,'C','K')
                                                ,'Q','O')
                                                ,'J','I')
                                                ,'0','O')
                                                ,'1','I')
                                                ,'2','Z')
                                                ,'3','E')
                                                ,'4','A')
                                                ,'5','S')
                                                ,'6','X')
                                                ,'7','Y')
                                                ,'8','B')
                                                ,'9','Q')
                                                ,'(.)\\\\1{1,}','$1') ,
                                                display
                                                FROM topxtagset WHERE CHAR_LENGTH(display)>2)
                                                SELECT * FROM topxtag ;
                                                                                       
                                                /*#step 2, join new tags in tag table.*/
                                                INSERT IGNORE INTO tag (idx,display)
                                                SELECT  nt.idx  ,
                                                        nt.display
                                                FROM new_tag nt
                                                LEFT JOIN  tag t USING(idx)
                                                WHERE t.tag IS NULL
                                                GROUP BY t.idx;
                                                                                                
                                                /*#step 3, get best 100 tags for every pro*/
                                                INSERT INTO pro_tag(pro,tag)
                                                WITH top AS 
                                                ( 
                                                    SELECT pro, ROW_NUMBER() OVER (
                                                            PARTITION BY pro
                                                            ORDER BY popularity DESC
                                                    ) order_num,popularity, tag
                                                    FROM new_tag
                                                    JOIN tag USING (idx)
                                                )                                                 
                                                SELECT top.pro, top.tag
                                                FROM top
                                                LEFT JOIN pro_tag pt USING (pro,tag)
                                                WHERE top.order_num <=100
                                                AND pt.tag IS NULL
                                                ;
                                                                                                
                                                TRUNCATE new_tag;
                                                                                                
                                                /*#thumb part*/
                                                INSERT IGNORE INTO thumb(pro, i, url)
                                                    SELECT pro,
                                                            n,
                                                            SUBSTRING_INDEX(SUBSTRING_INDEX(thumbs, ',', n), ',', -1)
                                                     FROM meta_changes
                                                              INNER JOIN thumb_numbers ON thumb_count >= n
                                                     ORDER BY pro, n
                                                    ;
                                                INSERT IGNORE INTO host ( domain, pointer, start, end, dl)
                                                SELECT domain,
                                                       MIN(pro),
                                                       MIN(pro),
                                                       MAX(pro),
                                                       COUNT(pro)     
                                                FROM thumb b 
                                                WHERE b.host IS NULL AND state = 0
                                                GROUP BY b.domain;
                                                                                                
                                                UPDATE thumb t
                                                    INNER JOIN host h USING (domain)
                                                SET t.host= h.host,
                                                    t.url   =SUBSTR(url FROM CHAR_LENGTH(SUBSTRING_INDEX(url, '/', 3)) + 1),
                                                    t.state = 1
                                                WHERE t.state=0;
                                                                                                
                                                UPDATE thumb t
                                                    LEFT OUTER JOIN host h USING (host)
                                                SET  t.state = 4
                                                WHERE t.state=0 
                                                AND h.host IS NULL;
                                                                   """)
                             .setHint(
                                     "javax.persistence.lock.timeout",
                                     3600
                                     )
                             .executeUpdate();

        for (Network network : Network.values()) {
            val changes = session.createNativeQuery("""
                                                    UPDATE %s n
                                                    JOIN host h       USING (domain)
                                                    JOIN meta_changes USING (pro)
                                                    JOIN pro p        USING (pro)
                                                    SET
                                                        n.$crc      = n.crc,
                                                        n.$crc_meta = n.crc_meta,
                                                        p.crc_meta  = n.crc_meta,
                                                        n.preview_d = NULL,
                                                        n.tag       = NULL,
                                                        n.cat       = NULL,
                                                        n.host      = h.host,
                                                        n.flag      = n.flag | 2,
                                                        p.updated   = DEFAULT,
                                                        n.updated   = DEFAULT
                                                    ;
                                                    """.formatted(network.tableName())
                                                   ).executeUpdate();
            log.info("Feedback: [{}] in timestamp:[{}]", changes);
        }
        notifier.displayTray("connect-new-entries-tags", "connect-new-entries-tags [ " + updates + " ]", MessageType.INFO);
    }
    public void updateEntities(Session session) {
        this.session = session;
        //this.insertRowsToProTable();
        this.connect();
    }
    @SneakyThrows
    public static void main(String[] args) {
        var job = new UpdateTags();
        HibernateUtill.run(job::updateEntities);
    }
}
