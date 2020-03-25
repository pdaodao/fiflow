package com.github.myetl.fiflow.core.frame;

import java.util.ArrayList;
import java.util.List;

public class FlowContext {
    private List<String> out = new ArrayList<>();

    public void outMsg(String msg, Object... args) {
        out.add(String.format(msg, args));
    }

}
