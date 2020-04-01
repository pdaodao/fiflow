package com.github.myetl.fiflow.web.controller;

import com.github.myetl.fiflow.web.model.SqlCmd;
import com.github.myetl.fiflow.web.model.SqlExecuteResult;
import com.github.myetl.fiflow.web.service.FiflowSqlService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

/**
 * 使用 fiflow 在 flink 中 执行 sql
 */
@RestController
@RequestMapping("/fisql")
public class FiflowSqlController {
    @Autowired
    private FiflowSqlService fiflowSqlService;

    @PostMapping("/run")
    public SqlExecuteResult runSql(@RequestBody SqlCmd sqlCmd) {
        try{
            return fiflowSqlService.run(sqlCmd);
        }catch (Exception e){
            e.printStackTrace();
        }

        return new SqlExecuteResult();
    }
}
