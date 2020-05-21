package com.github.lessonone.fiflow.core.util;

import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.table.delegation.Executor;
import org.apache.flink.table.operations.ModifyOperation;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.List;

public class TableEnvUtils {

    private static Field execEnvField = null;
    private static Field bufferedModifyOperationsField = null;
    private static Method translateMethod = null;

    public static Executor getExecutor(TableEnvironment tEnv) {
        if (tEnv == null || !(tEnv instanceof TableEnvironmentImpl)) return null;
        try {
            if (execEnvField == null) {
                execEnvField = TableEnvironmentImpl.class.getDeclaredField("execEnv");
                execEnvField.setAccessible(true);
            }
            return (Executor) execEnvField.get(tEnv);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public static List<ModifyOperation> getBufferedModifyOperations(TableEnvironment tEnv) {
        if (tEnv == null || !(tEnv instanceof TableEnvironmentImpl)) return null;
        try {
            if (bufferedModifyOperationsField == null) {
                bufferedModifyOperationsField = TableEnvironmentImpl.class.getDeclaredField("bufferedModifyOperations");
                bufferedModifyOperationsField.setAccessible(true);
            }
            return (List<ModifyOperation>) bufferedModifyOperationsField.get(tEnv);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public static void invokeTranslate(TableEnvironment tEnv) {
        if (tEnv == null || !(tEnv instanceof TableEnvironmentImpl)) return;
        try {
            if (translateMethod == null) {
                translateMethod = TableEnvironmentImpl.class.getDeclaredMethod("translate", List.class);
                translateMethod.setAccessible(true);
            }
            List<ModifyOperation> ops = getBufferedModifyOperations(tEnv);
            translateMethod.invoke(tEnv, ops);
            ops.clear();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
