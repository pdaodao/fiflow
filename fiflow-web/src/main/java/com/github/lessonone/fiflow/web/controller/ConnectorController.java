package com.github.lessonone.fiflow.web.controller;

import com.github.lessonone.fiflow.common.dao.ConnectorDao;
import com.github.lessonone.fiflow.common.entity.ConnectorEntity;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * 连接管理
 */
@RestController
@RequestMapping("/connector")
public class ConnectorController {
    @Autowired
    private ConnectorDao connectorDao;

    @GetMapping
    public List<ConnectorEntity> list(){
        return connectorDao.list();
    }



}
