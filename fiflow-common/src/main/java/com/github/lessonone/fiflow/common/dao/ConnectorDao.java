package com.github.lessonone.fiflow.common.dao;

import com.github.lessonone.fiflow.common.base.CommonMapper;
import com.github.lessonone.fiflow.common.entity.ConnectorEntity;


public class ConnectorDao extends BaseDao<ConnectorEntity> {

    public ConnectorDao(CommonMapper baseDao) {
        super(baseDao);
    }

    @Override
    protected String listFields() {
        return "id,name,comment,type_name,url,create_time,modify_time";
    }
}
