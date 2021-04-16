package nl.appmodel.realtime.model;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.EqualsAndHashCode.Include;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import javax.persistence.*;
@Entity
@Table(name = "pornhub", schema = "prosite", uniqueConstraints = @UniqueConstraint(columnNames = "keyid"))
@Slf4j
@Data
@EqualsAndHashCode(onlyExplicitlyIncluded = true)
@NoArgsConstructor
public class PornHub {
    @Id @Include @Column long   pornhub;
    @Column              String keyid;
    @Column              long   crc;
}
