package com.github.lessonone.fiflow.core.sql.builder.frame;

import com.github.lessonone.fiflow.core.flink.BuildLevel;
import com.github.lessonone.fiflow.core.flink.FlinkBuildInfo;
import com.github.lessonone.fiflow.core.sql.Cmd;
import com.github.lessonone.fiflow.core.sql.CmdBuilder;
import com.github.lessonone.fiflow.core.sql.SqlSessionContext;
import com.github.lessonone.fiflow.core.sql.builder.CmdBaseBuilder;
import com.github.lessonone.fiflow.core.util.JarUtils;
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
    public BuildLevel buildLevel() {
        return BuildLevel.Set;
    }

    @Override
    public FlinkBuildInfo build(FlinkBuildInfo result, Cmd cmd, SqlSessionContext session) {
        String jars = cmd.args[0];
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
