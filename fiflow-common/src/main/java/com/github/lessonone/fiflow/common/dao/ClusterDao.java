package com.github.lessonone.fiflow.common.dao;

import com.github.lessonone.fiflow.common.base.CommonMapper;
import com.github.lessonone.fiflow.common.base.SqlWrap;
import com.github.lessonone.fiflow.common.entity.ClusterEntity;

import java.util.List;
import java.util.Optional;

public class ClusterDao extends BaseDao<ClusterEntity> {

    public ClusterDao(CommonMapper commonMapper) {
        super(commonMapper);
    }

    public List<ClusterEntity> list(boolean isDelete) {
        return commonMapper.queryForList(
                SqlWrap.builder().select("*").from(clazz).where("is_delete").equal(isDelete).build());
    }

    public Optional<ClusterEntity> getByName(String name) {
        return commonMapper.queryForOne(
                SqlWrap.builder().select("*").from(clazz).where("name").equal(name).build());
    }
}
