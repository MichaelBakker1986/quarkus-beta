package nl.appmodel;

import lombok.AccessLevel;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.experimental.FieldDefaults;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;
@Data
@Entity
@Table(name = "Xtube", schema = "prosite")
@NoArgsConstructor
@EqualsAndHashCode(onlyExplicitlyIncluded = true)
@FieldDefaults(level = AccessLevel.PRIVATE)
public class Xtube {
    @Id
    @EqualsAndHashCode.Include
    @Column int    pro_id;
    @Column String code;
    @Column String url;
    @Column String picture_d;
    @Column String preview_d;
    @Column String header;
    @Column String tag;
    @Column String tag2;
    @Column String actor;
    @Column String amateur;
    @Column String cat;
    @Column long   duration;
    @Column long   views;
}
