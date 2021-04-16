package nl.appmodel.realtime;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;
import lombok.extern.java.Log;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
@Log
@ToString
@Getter
@AllArgsConstructor
public class SCVContext {
    URL          url;
    long         content_length_offset;
    long         content_length;
    UpdateStats  stats;
    List<String> sqlStatements;
    public SCVContext(URL url) {
        this.url           = url;
        this.stats         = new UpdateStats(0, 0, 0, 0);
        this.sqlStatements = new ArrayList<>();
    }
    public SCVContext withLength(long length) {
        this.content_length_offset = length;
        return this;
    }
    public void reset() {
        stats.reset();
        sqlStatements.clear();
    }
    public int getChanges()       { return stats.getChanges(); }
    public boolean noStatements() { return sqlStatements.isEmpty(); }
    public void add(String SQL) {
        this.sqlStatements.add(SQL);
    }
    public int addChanges(int more)                  { return this.stats.addChanges(more); }
    public void setContentLength(long contentLength) { this.content_length_offset = contentLength; }
    public int size()                                { return sqlStatements.size(); }
    public void clear()                              { sqlStatements.clear(); }
    public int getTotal()                            {return stats.getTotal(); }
    public int addTotal(int size)                    { return this.stats.total += size; }
    public long addExamined()                        { return stats.addExamined(); }
    public long addSkipped() {
        return stats.skipped++;
    }
    public long getExamined() { return stats.examinedRecords; }
    public boolean isEmpty()  { return sqlStatements.isEmpty(); }
    public long getSkipped() {
        return stats.skipped;
    }
    public SCVContext withTotalLength(long content_length) {
        this.content_length = content_length;
        return this;
    }
}
