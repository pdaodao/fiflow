package com.github.myetl.fiflow.core.sql;

import com.github.myetl.fiflow.core.core.FiflowSqlSession;
import com.github.myetl.fiflow.core.flink.BuildLevel;
import com.github.myetl.fiflow.core.flink.FlinkBuildInfo;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.Optional;

/**
 * sql command 转化为 flink 的操作
 */
public class SqlBuilder {
    public final FiflowSqlSession session;

    public final BuildContext buildContext = new BuildContext();
    public final BuildContext previousContext;

    public SqlBuilder(FiflowSqlSession session) {
        this.session = session;
        previousContext = new BuildContext();
        if(CollectionUtils.isNotEmpty(session.getSteps())){
            for(BuildContext step : session.getSteps()){
                previousContext.addCmdAll(step.getCmdList());
            }
        }
    }

    /**
     * 先预构建下 得到全局的信息
     *
     * @param sql
     * @return
     */
    public Cmd preBuild(String sql) {
        if(StringUtils.isBlank(sql)) return null;
        sql = sql.trim();

        for (CmdType cmdType : CmdType.values()) {
            Optional<String[]> accept = cmdType.cmdBuilder.accept(sql);
            if (accept.isPresent()) {
                Cmd cmd = new Cmd(cmdType, accept.get());
                cmd.preBuild(buildContext, previousContext);

                buildContext.addCmd(cmd);
                return cmd;
            }
        }
        throw new IllegalArgumentException("unknown sql type: " + sql);
    }

    /**
     * 在构建之前 利用全局信息做一些优化
     */
    private void beforeBuild() {
        // todo 判断是否从 kafa 读数据 从而需要切换到 streaming 模式
        // 此逻辑可能写在具体的 builder 中

        // 根据本次预构建的结果 确定是否使用 streaming 模式
        boolean isNewCreated = session.initEnv(buildContext.getStreamingMode());
        if(isNewCreated) {
            // 重放 set 和 create 操作 来构造环境
            for(Cmd cmd: previousContext.getCmdList()){
                if(cmd.level()  == BuildLevel.Set || cmd.level() == BuildLevel.Create){
                    cmd.build(session);
                }
            }
        }
    }

    /**
     * 把该批次的多行 sql 转换为 flink 中的操作
     *
     * @return
     */
    public FlinkBuildInfo build() {
        // 这里利用得到的全局信息 做一些优化
        beforeBuild();

        FlinkBuildInfo buildInfo = new FlinkBuildInfo(BuildLevel.None);
        for (Cmd cmd : buildContext.getCmdList()) {

            FlinkBuildInfo cmdBuildInfo = cmd.build(session);

            buildInfo = buildInfo.merge(cmdBuildInfo);

            if (buildInfo.getLevel() == BuildLevel.Error)
                return buildInfo;
        }
        return buildInfo;
    }
}
