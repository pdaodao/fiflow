package com.github.lessonone.fiflow.common.base;

import java.lang.annotation.*;

@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface TableField {

    String name() default "";

    boolean ignore() default true;

}
