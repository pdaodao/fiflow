package com.github.lessonone.fiflow.web.config;

import com.github.lessonone.fiflow.common.base.BaseDao;
import com.github.lessonone.fiflow.common.service.ClusterDao;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

import javax.sql.DataSource;

@Configuration
public class MvcConfig implements WebMvcConfigurer {

    @Bean
    public BaseDao baseDao(@Autowired DataSource dataSource){
        return new BaseDao(dataSource);
    }

    @Bean
    public ClusterDao clusterDao(@Autowired BaseDao baseDao){
        return new ClusterDao(baseDao);
    }
}
