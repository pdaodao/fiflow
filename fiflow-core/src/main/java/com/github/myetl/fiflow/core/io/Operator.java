package com.github.myetl.fiflow.core.io;

import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.ValueLiteralExpression;

import java.util.ArrayList;
import java.util.List;

public enum Operator {
    greaterThan(">"),
    greaterThanOrEqual(">="),
    lessThan("<"),

    like("like"),
    equals("=");


    private final String opt;

    Operator(String opt) {
        this.opt = opt;
    }

    public static Operator parse(String name){
        try{
            return Operator.valueOf(name);
        }catch (Exception e){

        }
        return null;
    }

    public String toSql(List<Expression> exps) {
        StringBuilder sb = new StringBuilder();
        List<Object> args = parseArgs(exps);
        sb.append('`').append(args.get(0)).append("`").append(" ").append(opt).append(" ").append(args.get(1));
        return sb.toString();
    }

    private List<Object> parseArgs(List<Expression> args){
        List<Object> ret  = new ArrayList<>(args.size());
        for(Expression exp : args){
            if(exp instanceof FieldReferenceExpression){
                ret.add(((FieldReferenceExpression) exp).getName());
                continue;
            }

            if(exp instanceof ValueLiteralExpression){
                ValueLiteralExpression vv = (ValueLiteralExpression) exp;
                if(vv.isNull()){
                    ret.add(null);
                }else{
                    ret.add(vv.getValueAs(vv.getOutputDataType().getConversionClass()).get());
                }
            }
        }

        return ret;
    }


}
