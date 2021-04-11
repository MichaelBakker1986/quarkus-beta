package nl.appmodel;

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
                targetClass = PornHubHash.class,
                columns = {
                        @ColumnResult(name = "pornhub", type = long.class),
                        @ColumnResult(name = "crc", type = long.class)}))
@Entity
@Immutable
public class PornHubHash {
    @Id long pornhub;
    long crc;
}
