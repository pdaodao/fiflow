package com.github.myetl.flow.core.inputformat;

/**
 * 构建 FlowInputFormat
 */
public interface InputFormatBuilder<T extends FlowInputFormat> {

    T build();
}