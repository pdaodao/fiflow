package com.github.lessonone.fiflow.common.entity;

import com.fasterxml.jackson.annotation.JsonFormat;
import lombok.Data;

import java.util.Date;

@Data
public class BaseEntity {
    private Long id;
    @JsonFormat(pattern = "yyyy-MM-dd hh:MM:ss")
    private Date createTime;
    @JsonFormat(pattern = "yyyy-MM-dd hh:MM:ss")
    private Date modifyTime;
}
