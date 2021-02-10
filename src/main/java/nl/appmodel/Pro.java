package nl.appmodel;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.EqualsAndHashCode.Include;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import javax.persistence.*;
@Entity
@Table(name = "pro", schema = "prosite")
@Slf4j
@Data
@EqualsAndHashCode(onlyExplicitlyIncluded = true)
@NoArgsConstructor
public class Pro {
    @Id @GeneratedValue @Include long    id;
    @Column                      String  thumbs;
    @Column(nullable = false)    boolean downloaded = true;
}
