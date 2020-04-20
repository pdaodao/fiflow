package com.github.myetl.fiflow.core.util;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class JarUtils {


    public static List<URL> jars(String... jars) throws IOException {
        List<URL> result = new ArrayList<>();
        if (jars == null || jars.length < 1)
            return result;

        Set<String> sets = new HashSet<>();

        String[] cps = System.getProperty("java.class.path").split(":");
        for (String name : cps) {
            for (String t : jars) {
                if (name.contains(t.toLowerCase().trim())) {
                    if (!sets.contains(name)) {
                        result.add(new File(name).toURI().toURL());
                        sets.add(name);
                        break;
                    }
                }
            }
        }

        return result;
    }


}
