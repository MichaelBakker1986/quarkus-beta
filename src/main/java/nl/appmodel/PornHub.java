package nl.appmodel;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.EqualsAndHashCode.Include;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import javax.persistence.*;
@Entity
@Table(name = "pornhub", schema = "prosite", uniqueConstraints = @UniqueConstraint(columnNames = "pornhub_id"))
@Slf4j
@Data
@EqualsAndHashCode(onlyExplicitlyIncluded = true)
@NoArgsConstructor
public class PornHub {
    @Id @GeneratedValue @Include String keyid;
    @Include @Column             long   pornhub_id;
    @Column                      String changes_hash;
}
