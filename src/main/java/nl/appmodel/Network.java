package nl.appmodel;

import lombok.AllArgsConstructor;
import lombok.Getter;
@AllArgsConstructor
@Getter
public enum Network {
    PornHub("preview_d",
            "prosite.iframe(w,h,CONCAT('https://www.pornhub.com/embed/',keyid)) ",
            "IFNULL(w,-1)",
            "IFNULL(h,-1)",
            "deleted",
            "TRIM(BOTH ',' FROM CONCAT(IFNULL(prosite.hash_tag_split(tag),''),',',IFNULL(prosite.hash_tag_split(cat),'')))",
            PornhubDTO.class),
    xtube("preview_m",
          "code",
          "-1",
          "-1",
          "false",
          "TRIM(BOTH ',' FROM  CONCAT(IFNULL(prosite.hash_tag_split(tag),''),',',IFNULL(prosite.hash_tag_split(tag2),''),',',IFNULL(prosite.hash_tag_split(cat),'')))",
          XtubeDTO.class),
    YouPorn("picture_m",
            "code",
            "-1",
            "-1",
            "false",
            "TRIM(BOTH ',' FROM CONCAT(IFNULL(prosite.hash_tag_split(tag),''),',',IFNULL(prosite.hash_tag_split(cat),'')))",
            YoupornDTO.class),
    Xvideos("picture_m",
            "code",
            "-1",
            "-1",
            "false",
            "TRIM(BOTH ',' FROM CONCAT(IFNULL(prosite.hash_tag_split(tag),''),',',IFNULL(prosite.hash_tag_split(cat),'')))",
            XVideoDTO.class);
    String                    thumb_col;
    String                    code;
    String                    w;
    String                    h;
    String                    deleted;
    String                    tag_set;
    Class<? extends TagAndId> dto;
    public String tableName() {
        return "prosite." + name().toLowerCase();
    }
    public Object getTagSetJoiner() {return tag_set; }
}
