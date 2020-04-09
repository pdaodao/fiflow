package com.github.myetl.fiflow.web.controller;


import com.github.myetl.fiflow.core.sql.BuildLevel;
import com.github.myetl.fiflow.core.sql.CmdBuildInfo;
import com.github.myetl.fiflow.web.model.SqlCmd;
import com.github.myetl.fiflow.web.service.FiflowSqlService;
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
    public CmdBuildInfo runSql(@RequestBody SqlCmd sqlCmd) {
        try {
            return fiflowSqlService.run(sqlCmd);
        } catch (Exception e) {
            e.printStackTrace();
            CmdBuildInfo result = new CmdBuildInfo(BuildLevel.Error);
            result.addMsg(ExceptionUtils.getStackTrace(e));
            return result;
        }
    }
}
