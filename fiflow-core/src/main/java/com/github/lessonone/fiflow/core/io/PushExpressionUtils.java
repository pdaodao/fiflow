package com.github.lessonone.fiflow.core.io;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.Expression;

import java.util.ArrayList;
import java.util.List;

public class PushExpressionUtils {

    public static String toWhere(List<Expression> predicates, String logical) {
        List<String> ret = new ArrayList<>();
        if (logical == null) {
            logical = "and";
        }

        for (Expression exp : predicates) {
            if (exp instanceof CallExpression) {
                CallExpression call = (CallExpression) exp;
                final String functionName;
                if (!call.getFunctionIdentifier().isPresent()) {
                    functionName = call.getFunctionDefinition().toString();
                } else {
                    functionName = call.getFunctionIdentifier().get().asSummaryString();
                }

                if (functionName == "or") {
                    String sub = toWhere(call.getChildren(), "or");
                    if (sub != null) {
                        ret.add(sub);
                    }
                    continue;
                }

                Operator operator = Operator.parse(functionName);
                if (operator != null) {
                    ret.add(operator.toSql(call.getChildren()));
                }
            }
        }
        if (ret.size() == 0) return null;
        if (ret.size() == 1) return ret.get(0);
        return "( " + StringUtils.join(ret, " " + logical + " ") + " )";
    }
}
