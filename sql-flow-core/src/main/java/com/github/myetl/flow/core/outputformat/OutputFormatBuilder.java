package com.github.myetl.flow.core.outputformat;

/**
 * 构建 FlowOutputFormat
 */
public interface OutputFormatBuilder<T extends FlowOutputFormat> {

    T build();
}