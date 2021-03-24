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
        var executionUpdateTime = System.currentTimeMillis();
        for (Network network : Network.values()) {
            val updates = session.createNativeQuery("""
                                                    UPDATE %s x set x.ref = UUID_SHORT(),x.updated = :updated where x.pro_id IS NULL AND x.ref IS NULL
                                                        """.formatted(network.tableName())
                                                   ).setParameter("updated", -executionUpdateTime)
                                 .executeUpdate();

            val changes = session.createNativeQuery("""
                                                    INSERT INTO prosite.pro (id, thumbs, downloaded, views, tag_set, header, embed, w, h, status, duration,ref,updated) 
                                                    (SELECT null,%s,n.status=2,IFNULL(n.views,-1),%s,header,%s,%s,%s,1,IFNULL(n.duration,-1),ref,:updated From %s n where n.pro_id IS NULL AND n.ref IS NOT NULL)
                                                    """.formatted(network.getThumb_col(), network.getTag_set_new(), network.getCode_new(),
                                                                  network.getW(), network.getH(),
                                                                  network.tableName())
                                                   )
                                 .setParameter("updated", -executionUpdateTime)
                                 .executeUpdate();
            val pro_id = session.createNativeQuery("""
                                                   UPDATE %s x
                                                   JOIN prosite.pro p on p.ref=x.ref 
                                                   set x.pro_id = p.id, 
                                                        x.ref   = null,                                             
                                                        x.updated =  ?   
                                                   WHERE x.pro_id IS NULL
                                                   """.formatted(network.tableName())
                                                  ).setParameter(1, executionUpdateTime).executeUpdate();
            log.info("Feedback: [{}] [{}] [{}] in timestamp:[{}]", updates, changes, pro_id, executionUpdateTime);
        }

        log.info("Done with timestamp:[{}]", executionUpdateTime);
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
                                                 Synchronizing script new PRO entities into tags\s
                                                 and pro_tags combinations
                                                */
                                                SET @current_millis = prosite.currentmillis();
                                                DROP   TABLE IF EXISTS tmp_tags;
                                                CREATE   TABLE tmp_tags(id int PRIMARY KEY, idx VARCHAR(64) ,popularity bigint, index (idx) VISIBLE, index (popularity) VISIBLE) ENGINE=MEMORY
                                                SELECT  t.id,t.idx,t.popularity from prosite.tags t;
                                                       
                                                DROP TABLE IF EXISTS tmp_pro;
                                                CREATE TEMPORARY TABLE tmp_pro(pro_id int PRIMARY KEY,tag_set VARCHAR(1024))  ENGINE = MEMORY;
                                                INSERT INTO tmp_pro (SELECT id pro_id,tag_set as tag_set from prosite.pro WHERE STATUS = 1 OR status = 4);
                                                       
                                                /*
                                                 Extract tags from pro table
                                                */                
                                                DROP TABLE IF EXISTS my_tmp_table;
                                                CREATE TABLE my_tmp_table(pro_id int,idx VARCHAR(64),orig VARCHAR(64), index (idx) VISIBLE,PRIMARY KEY (pro_id,idx))  ENGINE = MEMORY;
                                                INSERT IGNORE INTO my_tmp_table
                                                 (SELECT
                                                  nw.pro_id AS pro_id,
                                                  prosite.hash_tag_index(SUBSTRING_INDEX(SUBSTRING_INDEX(nw.tag_set, ',', numbers.n), ',', -1)) idx,
                                                  SUBSTRING_INDEX(SUBSTRING_INDEX(nw.tag_set, ',', numbers.n), ',', -1) orig
                                                FROM
                                                  (
                                                  SELECT 1 n UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8
                                                 UNION ALL SELECT 9 UNION ALL SELECT 10 UNION ALL SELECT 11 UNION ALL SELECT 12 UNION ALL SELECT 13 UNION ALL SELECT 14 UNION ALL SELECT 15 UNION ALL SELECT 16
                                                 UNION ALL SELECT 17 UNION ALL SELECT 18 UNION ALL SELECT 19 UNION ALL SELECT 20 UNION ALL SELECT 21 UNION ALL SELECT 22 UNION ALL SELECT 23 UNION ALL SELECT 24
                                                 UNION ALL SELECT 25 UNION ALL SELECT 26 UNION ALL SELECT 27 UNION ALL SELECT 28 UNION ALL SELECT 29 UNION ALL SELECT 30 UNION ALL SELECT 31 UNION ALL SELECT 32
                                                 UNION ALL SELECT 33 UNION ALL SELECT 34 UNION ALL SELECT 35 UNION ALL SELECT 36 UNION ALL SELECT 37 UNION ALL SELECT 38 UNION ALL SELECT 39 UNION ALL SELECT 40
                                                 UNION ALL SELECT 41 UNION ALL SELECT 42 UNION ALL SELECT 43 UNION ALL SELECT 44 UNION ALL SELECT 45 UNION ALL SELECT 46 UNION ALL SELECT 47 UNION ALL SELECT 48
                                                 UNION ALL SELECT 49 UNION ALL SELECT 50 UNION ALL SELECT 51 UNION ALL SELECT 52 UNION ALL SELECT 53 UNION ALL SELECT 54 UNION ALL SELECT 55 UNION ALL SELECT 56
                                                 UNION ALL SELECT 57 UNION ALL SELECT 58 UNION ALL SELECT 59 UNION ALL SELECT 60 UNION ALL SELECT 61 UNION ALL SELECT 62 UNION ALL SELECT 63 UNION ALL SELECT 64
                                                 UNION ALL SELECT 65 UNION ALL SELECT 66 UNION ALL SELECT 67 UNION ALL SELECT 68 UNION ALL SELECT 69 UNION ALL SELECT 70 UNION ALL SELECT 71 UNION ALL SELECT 72
                                                 UNION ALL SELECT 73 UNION ALL SELECT 74 UNION ALL SELECT 75 UNION ALL SELECT 76 UNION ALL SELECT 77 UNION ALL SELECT 78 UNION ALL SELECT 79 UNION ALL SELECT 80
                                                 UNION ALL SELECT 81 UNION ALL SELECT 82 UNION ALL SELECT 83 UNION ALL SELECT 84 UNION ALL SELECT 85 UNION ALL SELECT 86 UNION ALL SELECT 87 UNION ALL SELECT 88
                                                 UNION ALL SELECT 89 UNION ALL SELECT 90 UNION ALL SELECT 91 UNION ALL SELECT 92 UNION ALL SELECT 93 UNION ALL SELECT 94 UNION ALL SELECT 95 UNION ALL SELECT 96
                                                 UNION ALL SELECT 97 UNION ALL SELECT 98 UNION ALL SELECT 99 UNION ALL SELECT 100 UNION ALL SELECT 101 UNION ALL SELECT 102 UNION ALL SELECT 103 UNION ALL SELECT 104
                                                   ) numbers INNER JOIN tmp_pro nw
                                                  ON CHAR_LENGTH(nw.tag_set)
                                                     -CHAR_LENGTH(REPLACE(nw.tag_set, ',', ''))>=numbers.n-1
                                                                               
                                                 ORDER BY  pro_id, n
                                                 ) ;
                                                /*
                                                 insert new Tags
                                                */
                                                INSERT IGNORE INTO prosite.tags
                                                SELECT null AS id,
                                                        mtt.idx AS name,
                                                        REGEXP_SUBSTR(mtt.idx, '[a-z0-9]{2}') short_2,
                                                        REGEXP_SUBSTR(mtt.idx, '[a-z0-9]{3}') short_3,
                                                        REGEXP_SUBSTR(mtt.idx, '[a-z0-9]{4}') short_4,
                                                        REGEXP_SUBSTR(mtt.idx, '[a-z0-9]{5}') short_5,
                                                        -1 AS used,
                                                        -1 AS popularity,
                                                         mtt.orig display_name,
                                                        REGEXP_SUBSTR(mtt.idx, '[a-z0-9]{6}') short_6,
                                                        REGEXP_SUBSTR(mtt.idx, '[a-z0-9]{7}') short_7,
                                                        mtt.idx as idx,
                                                        -@current_millis as updated        
                                                from my_tmp_table mtt
                                                LEFT JOIN tmp_tags tt
                                                ON tt.idx = mtt.idx
                                                WHERE tt.id IS NULL  AND char_length(mtt.idx)>2;     
                                                                 
                                                TRUNCATE tmp_tags;
                                                INSERT INTO tmp_tags SELECT  t.id,t.idx,t.popularity from prosite.tags t;
                                                                 
                                                /*
                                                 get best 100 of all entriess.. JOIN with TAGs to get the ID with the PRO
                                                */
                                                DROP TEMPORARY TABLE IF EXISTS tmp_pro_tags;
                                                CREATE TEMPORARY TABLE tmp_pro_tags(pro int,tag int,PRIMARY KEY my_tmp_pk (pro , tag)) ENGINE=MEMORY;
                                                INSERT IGNORE INTO tmp_pro_tags SELECT x2.pro_id pro,TT.id tag from my_tmp_table x2 ,tmp_tags TT,
                                                (select x.pro_id, ROW_NUMBER() OVER (
                                                            PARTITION BY pro_id
                                                            ORDER BY popularity DESC
                                                    ) order_num,popularity, x.idx
                                                    FROM my_tmp_table x
                                                    INNER JOIN  tmp_tags t ON x.idx=t.idx)    top_n
                                                WHERE (x2.pro_id,x2.idx)=(top_n.pro_id,top_n.idx)
                                                AND top_n.order_num <100
                                                AND TT.idx=top_n.idx;
                                                /*
                                                 Add the found links
                                                */
                                                INSERT INTO prosite.pro_tags
                                                SELECT tpt.pro,tpt.tag from tmp_pro_tags tpt
                                                LEFT JOIN prosite.pro_tags pt
                                                ON (tpt.pro,tpt.tag) = (pt.pro,pt.tag)
                                                WHERE pt.pro IS NULL;
                                                                 
                                                /*                      
                                                 Update visited records to next stage
                                                 Updated TAGS for most-used,most-popular                       
                                                */
                                                UPDATE prosite.tags t INNER JOIN tmp_pro_tags tpt
                                                ON t.id = tpt.tag 
                                                SET t.updated = -@current_millis;
                                                /*                      
                                                 Update visited records to next stage
                                                */
                                                UPDATE prosite.pro p INNER JOIN tmp_pro
                                                ON p.id= tmp_pro.pro_id
                                                SET 
                                                    p.status = IF(downloaded, 2, 3),
                                                    p.updated = @current_millis;
                                                                                   
                                                DROP TABLE IF EXISTS tmp_tags;
                                                DROP TABLE IF EXISTS tmp_pro;
                                                DROP TABLE IF EXISTS my_tmp_table;
                                                DROP TEMPORARY TABLE IF EXISTS tmp_pro_tags;
                                                                   """)
                             .setHint(
                                     "javax.persistence.lock.timeout",
                                     3600
                                     )
                             .executeUpdate();
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
