package com.github.lessonone.fiflow.web.controller;

import com.github.lessonone.fiflow.common.entity.ClusterEntity;
import com.github.lessonone.fiflow.common.service.ClusterDao;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;

/**
 * flink 集群管理
 */
@RestController
@RequestMapping("/cluster")
public class ClusterController {
    @Autowired
    private ClusterDao clusterDao;

    @PostMapping
    public Long save(@RequestBody ClusterEntity clusterEntity){
        return clusterDao.save(clusterEntity);
    }

    @GetMapping
    public List<ClusterEntity> list(){
        return clusterDao.list(false);
    }

    @GetMapping("/{id}")
    public ClusterEntity info(@PathVariable(name = "id") Long id){
        return clusterDao.getById(id).orElse(null);
    }

}
