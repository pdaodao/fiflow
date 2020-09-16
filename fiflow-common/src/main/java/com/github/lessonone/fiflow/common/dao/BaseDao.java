package com.github.lessonone.fiflow.common.dao;

import com.github.lessonone.fiflow.common.base.CommonMapper;
import com.github.lessonone.fiflow.common.base.SqlWrap;
import com.github.lessonone.fiflow.common.entity.BaseEntity;

import java.lang.reflect.ParameterizedType;
import java.util.List;
import java.util.Optional;

public abstract class BaseDao<T extends BaseEntity> {
    protected final CommonMapper commonMapper;
    protected final Class clazz;

    public BaseDao(CommonMapper commonMapper) {
        this.commonMapper = commonMapper;
        this.clazz = (Class<T>) ((ParameterizedType) this.getClass().getGenericSuperclass()).getActualTypeArguments()[0];
        System.out.println(clazz.getName());
    }

    protected String listFields() {
        return "*";
    }

    public List<T> list() {
        return commonMapper.queryForList(SqlWrap.builder().select(listFields()).from(clazz).build());
    }

    public Long save(T entity) {
        if (entity.getId() != null) {
            commonMapper.update(entity, true);
            return entity.getId();
        }
        return commonMapper.insert(entity, true);
    }

    public Optional<T> getById(Long id) {
        return commonMapper.queryForOne(SqlWrap.builder().select("*")
                .from(clazz).where("id").equal(id).build());
    }

}
