package com.github.lessonone.fiflow.web.config;

import com.github.lessonone.fiflow.common.base.CommonMapper;
import com.github.lessonone.fiflow.common.dao.ClusterDao;
import com.github.lessonone.fiflow.common.dao.ConnectorDao;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

import javax.sql.DataSource;

@Configuration
public class MvcConfig implements WebMvcConfigurer {

    @Bean
    public CommonMapper commonMapper(@Autowired DataSource dataSource){
        return new CommonMapper(dataSource);
    }

    @Bean
    public ClusterDao clusterDao(@Autowired CommonMapper commonMapper){
        return new ClusterDao(commonMapper);
    }

    @Bean
    public ConnectorDao connectorDao(@Autowired CommonMapper commonMapper){
        return new ConnectorDao(commonMapper);
    }
}
