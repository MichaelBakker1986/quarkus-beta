package nl.appmodel.realtime;

import io.quarkus.scheduler.Scheduled;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import nl.appmodel.Network;
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
                                                SELECT
                                                pro,
                                                REGEXP_REPLACE(
                                                REPLACE(
                                                REPLACE(
                                                REPLACE(
                                                REPLACE(
                                                REPLACE(
                                                REPLACE(
                                                REPLACE(
                                                REPLACE(
                                                REGEXP_REPLACE(
                                                LOWER(SUBSTRING_INDEX(SUBSTRING_INDEX(tag_set, ',', n), ',', -1))
                                                ,'[^a-z0-9]','')
                                                ,'5','s')
                                                ,'2','z')
                                                ,'8','b')
                                                ,'9','q')
                                                ,'y','i')
                                                ,'0','o')
                                                ,'1','i')
                                                ,'3','e')
                                                ,'4','a')
                                                ,'(.)\\\\1{1,}','$1') ,
                                                SUBSTRING_INDEX(SUBSTRING_INDEX(tag_set, ',', n), ',', -1) 
                                                FROM tag_numbers
                                                JOIN meta_changes ON tag_count > n             
                                                ORDER BY  pro, n;
                                                                                                
                                                /*step 1a, delete invalid tags..*/
                                                DELETE from new_tag where char_length(idx)<=2;
                                                                                                
                                                /*#step 2, join new tags in tag table.*/
                                                INSERT IGNORE INTO tag (idx,display)
                                                SELECT  nt.idx  ,
                                                        nt.display
                                                FROM new_tag nt
                                                LEFT JOIN  tag t USING(idx)
                                                WHERE t.tag IS NULL
                                                GROUP by t.idx;
                                                                                                
                                                /*#step 3, get best 100 tags for every pro*/
                                                INSERT INTO pro_tag(pro,tag)
                                                WITH top AS 
                                                ( 
                                                    select pro, ROW_NUMBER() OVER (
                                                            PARTITION BY pro
                                                            ORDER BY popularity DESC
                                                    ) order_num,popularity, tag
                                                    FROM new_tag
                                                    JOIN tag USING (idx)
                                                )                                                 
                                                SELECT top.pro, top.tag
                                                FROM top
                                                LEFT JOIN pro_tag pt USING (pro,tag)
                                                WHERE top.order_num <=100;
                                                                                                
                                                TRUNCATE new_tag;
                                                                                                
                                                /*#thumb part*/
                                                INSERT IGNORE INTO thumb(pro, i, url)
                                                    SELECT pro,
                                                            n,
                                                            SUBSTRING_INDEX(SUBSTRING_INDEX(thumbs, ',', n), ',', -1)
                                                     FROM meta_changes
                                                              INNER JOIN thumb_numbers ON thumb_count > n - 1
                                                     ORDER BY pro, n
                                                    ;
                                                INSERT IGNORE INTO host ( domain, pointer, start, end, dl)
                                                SELECT domain,
                                                       min(pro),
                                                       min(pro),
                                                       max(pro),
                                                       count(pro)     
                                                FROM thumb b 
                                                WHERE b.host IS NULL AND state = 0
                                                GROUP by b.domain;
                                                                                                
                                                UPDATE thumb t
                                                    INNER JOIN host h USING (domain)
                                                SET t.host= h.host,
                                                    t.url   =SUBSTR(url FROM char_length(SUBSTRING_INDEX(url, '/', 3)) + 1),
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
                                                    update %s n
                                                    JOIN host h       USING (domain)
                                                    JOIN meta_changes USING (pro)
                                                    JOIN pro p        USING (pro)
                                                    SET
                                                        n.$crc      = n.crc,
                                                        n.$crc_meta = n.crc_meta,
                                                        p.crc_meta  = n.crc_meta,
                                                        n.preview_d = null,
                                                        n.tag       = null,
                                                        n.cat       = null,
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
        HibernateUtil.run(job::updateEntities);
    }
}
