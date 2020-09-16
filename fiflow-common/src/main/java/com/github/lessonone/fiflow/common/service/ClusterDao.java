package com.github.lessonone.fiflow.common.service;

import com.github.lessonone.fiflow.common.base.BaseDao;
import com.github.lessonone.fiflow.common.base.SqlWrap;
import com.github.lessonone.fiflow.common.entity.ClusterEntity;

import java.util.List;
import java.util.Optional;

public class ClusterDao {
    private final BaseDao baseDao;

    public ClusterDao(BaseDao baseDao) {
        this.baseDao = baseDao;
    }

    public Long save(ClusterEntity clusterEntity){
        if(clusterEntity.getId() != null){
            baseDao.update(clusterEntity, true);
            return clusterEntity.getId();
        }
        return baseDao.insert(clusterEntity, true);
    }

    private SqlWrap.SqlWrapBuilder baseSelect(){
        return SqlWrap.builder().select("*").from(ClusterEntity.class);
    }

    public List<ClusterEntity> list(Boolean isDelete){
        SqlWrap.SqlWrapBuilder builder = baseSelect();
        if(isDelete != null){
            builder.where("is_delete").equal(isDelete);
        }
        return baseDao.queryForList(builder.build());
    }

    public Optional<ClusterEntity> getById(Long id){
        return baseDao.queryForOne(baseSelect().where("id").equal(id).build());
    }

    public Optional<ClusterEntity> getByName(String name){
        return baseDao.queryForOne(baseSelect().where("name").equal(name).build());
    }
}
