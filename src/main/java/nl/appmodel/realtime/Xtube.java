package nl.appmodel.realtime;

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
    @Column String pictureM;
    @Column String previewM;
    @Column String header;
    @Column String tag;
    @Column String tag2;
    @Column String c9;
    @Column String c10;
    @Column String cat;
    @Column long   duration;
    @Column long   views;
}
