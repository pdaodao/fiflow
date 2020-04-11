package com.github.myetl.fiflow.core.sql.builder.frame;

import com.github.myetl.fiflow.core.core.FiflowSqlSession;
import com.github.myetl.fiflow.core.flink.BuildLevel;
import com.github.myetl.fiflow.core.sql.Cmd;
import com.github.myetl.fiflow.core.flink.FlinkBuildInfo;
import com.github.myetl.fiflow.core.sql.CmdBuilder;
import com.github.myetl.fiflow.core.sql.builder.CmdBaseBuilder;
import com.github.myetl.fiflow.core.util.JarUtils;
import org.apache.commons.lang3.StringUtils;

import java.net.URL;
import java.util.List;

/**
 * jar a,b
 * 添加依赖jar
 */
public class JarBuilder extends CmdBaseBuilder implements CmdBuilder {
    public static final String pattern = "-*\\s?jar\\s+(.*)";

    public JarBuilder() {
        super(pattern);
    }

    @Override
    public String help() {
        return "jar x,y; add dependent JAR files";
    }

    @Override
    public FlinkBuildInfo build(Cmd cmd, FiflowSqlSession session) {
        String jars = cmd.args[0];
        FlinkBuildInfo result = new FlinkBuildInfo(BuildLevel.Set);
        if (StringUtils.isEmpty(jars)) {
            result.addMsg("jar is empty");
        }
        String[] arrs = jars.split(",|;");
        try {
            List<URL> urls = JarUtils.jars(arrs);
            for (URL url : urls) {
                result.addMsg("add classpath jar " + url.getPath());
            }
            for (String jarName : arrs) {
                session.addJar(jarName);
            }
        } catch (Exception e) {
            FlinkBuildInfo error = new FlinkBuildInfo(BuildLevel.Error);
            error.addMsg("add jar " + jars);
            error.addMsg(e.getMessage());
            result = result.merge(error);
        }
        return result;
    }
}
