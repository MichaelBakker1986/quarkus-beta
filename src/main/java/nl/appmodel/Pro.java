package nl.appmodel;

/*import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.EqualsAndHashCode.Include;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;*/
import javax.persistence.*;
import java.util.Objects;
@Entity
@Table(name = "pro", schema = "prosite")
/*@Slf4j
@Data
@EqualsAndHashCode(onlyExplicitlyIncluded = true)
@NoArgsConstructor*/
public class Pro {
    @Id @GeneratedValue
    /*@Include*/
            long id;
    @Column                   String  thumbs;
    @Column                   byte[]  ref;
    @Column(nullable = false) boolean downloaded = true;
    public String getSuffix() {
        return ".jpg";
    }
    public String getFileName() {
        var s        = thumbs.split("[,;]")[0];
        var filename = s.substring(s.lastIndexOf("/") + 1);
        return filename.substring(0, filename.length() - 4);
    }
    @Override public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Pro pro = (Pro) o;
        return id == pro.id;
    }
    @Override public int hashCode() {
        return Objects.hash(id);
    }
}
