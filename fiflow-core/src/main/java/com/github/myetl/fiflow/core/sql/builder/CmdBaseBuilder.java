package com.github.myetl.fiflow.core.sql.builder;

import com.github.myetl.fiflow.core.sql.CmdBuilder;
import org.apache.commons.lang3.StringUtils;

import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public abstract class CmdBaseBuilder implements CmdBuilder {
    public final Pattern regPattern;

    public CmdBaseBuilder(String p) {
        this.regPattern = Pattern.compile(p, DEFAULT_PATTERN_FLAGS);
    }

    @Override
    public Optional<String[]> accept(String sql) {
        if (StringUtils.isBlank(sql) || regPattern == null) return Optional.empty();
        final Matcher matcher = regPattern.matcher(sql);
        if (matcher.matches()) {
            final String[] groups = new String[matcher.groupCount()];
            for (int i = 0; i < groups.length; i++) {
                groups[i] = matcher.group(i + 1);
            }
            return Optional.of(groups);
        }
        return Optional.empty();
    }

}
