package nl.appmodel.realtime;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;
import lombok.extern.java.Log;
import java.util.HashSet;
import java.util.Set;
@Log
@ToString
@Getter
@AllArgsConstructor
public class UpdateStats {
    private       int       changes = 0;
    private final Set<Long> seenIds = new HashSet<>();
    int total = 0;
    public long skipped = 0, examinedRecords = 0;
    public void reset() {
        changes = 0;
    }
    public int addChanges(int more) { return (this.changes += more); }
    public long addExamined() {
        return ++examinedRecords;
    }
}
