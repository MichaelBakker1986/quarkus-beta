package nl.appmodel;

import lombok.AllArgsConstructor;
import lombok.Getter;
@AllArgsConstructor
@Getter
public enum Network {
    PornHub("preview_d",
            "prosite.iframe(w,h,CONCAT('https://www.pornhub.com/embed/',keyid)) ",
            "prosite.iframe(n.w,n.h,CONCAT('https://www.pornhub.com/embed/',n.keyid)) ",
            "IFNULL(w,-1)",
            "IFNULL(n.w,-1)",
            "IFNULL(h,-1)",
            "IFNULL(n.h,-1)",
            "deleted",
            "TRIM(BOTH ',' FROM CONCAT(IFNULL(prosite.hash_tag_split(tag),''),',',IFNULL(prosite.hash_tag_split(cat),'')))",
            "TRIM(BOTH ',' FROM CONCAT(IFNULL(prosite.hash_tag_split(n.tag),''),',',IFNULL(prosite.hash_tag_split(n.cat),'')))",
            PornhubDTO.class),
    xtube("preview_m",
          "code",
          "n.code",
          "-1",
          "-1",
          "-1",
          "-1",
          "false",
          "TRIM(BOTH ',' FROM  CONCAT(IFNULL(prosite.hash_tag_split(tag),''),',',IFNULL(prosite.hash_tag_split(tag2),''),',',IFNULL(prosite.hash_tag_split(cat),'')))",
          "TRIM(BOTH ',' FROM  CONCAT(IFNULL(prosite.hash_tag_split(n.tag),''),',',IFNULL(prosite.hash_tag_split(n.tag2),''),',',IFNULL(prosite.hash_tag_split(n.cat),'')))",
          XtubeDTO.class),
    YouPorn("picture_m",
            "code",
            "n.code",
            "-1",
            "-1",
            "-1",
            "-1",
            "false",
            "TRIM(BOTH ',' FROM CONCAT(IFNULL(prosite.hash_tag_split(tag),''),',',IFNULL(prosite.hash_tag_split(cat),'')))",
            "TRIM(BOTH ',' FROM CONCAT(IFNULL(prosite.hash_tag_split(n.tag),''),',',IFNULL(prosite.hash_tag_split(n.cat),'')))",
            YoupornDTO.class),
    Xvideos("picture_m",
            "code",
            "n.code",
            "-1",
            "-1",
            "-1",
            "-1",
            "false",
            "TRIM(BOTH ',' FROM CONCAT(IFNULL(prosite.hash_tag_split(tag),''),',',IFNULL(prosite.hash_tag_split(cat),'')))",
            "TRIM(BOTH ',' FROM CONCAT(IFNULL(prosite.hash_tag_split(n.tag),''),',',IFNULL(prosite.hash_tag_split(n.cat),'')))",
            XVideoDTO.class);
    String                    thumb_col;
    String                    code;
    String                    code_new;
    String                    w;
    String                    w_new;
    String                    h;
    String                    h_new;
    String                    deleted;
    String                    tag_set;
    String                    tag_set_new;
    Class<? extends TagAndId> dto;
    public String tableName() {
        return "prosite." + name().toLowerCase();
    }
    public Object getTagSetJoiner()    {return tag_set; }
    public Object getTagSetJoinerNew() {return tag_set_new; }
}
