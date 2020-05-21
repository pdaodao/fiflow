package com.github.lessonone.fiflow.web.controller;


import com.github.lessonone.fiflow.core.flink.BuildLevel;
import com.github.lessonone.fiflow.core.flink.FlinkBuildInfo;
import com.github.lessonone.fiflow.web.model.SqlCmd;
import com.github.lessonone.fiflow.web.service.FiflowSqlService;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * 使用 fiflow 在 flink 中 执行 sql
 */
@RestController
@RequestMapping("/fisql")
public class FiflowSqlController {
    @Autowired
    private FiflowSqlService fiflowSqlService;

    @PostMapping("/run")
    public FlinkBuildInfo runSql(@RequestBody SqlCmd sqlCmd) {
        try {
            return fiflowSqlService.run(sqlCmd);
        } catch (Exception e) {
            e.printStackTrace();
            FlinkBuildInfo result = new FlinkBuildInfo(BuildLevel.Error);
            result.addMsg(ExceptionUtils.getStackTrace(e));
            return result;
        }
    }
}
