package nl.appmodel.realtime.model;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.hibernate.annotations.Immutable;
import javax.persistence.*;
@AllArgsConstructor
@Getter
@NoArgsConstructor
@SqlResultSetMapping(
        name = "REMAP",
        classes = @ConstructorResult(
                targetClass = NetworkHash.class,
                columns = {
                        @ColumnResult(name = "pornhub", type = long.class),
                        @ColumnResult(name = "crc", type = long.class)}))
@Entity
@Immutable
public class NetworkHash {
    @Id long pornhub;
    long crc;
    public long getHash() { return pornhub; }
}
