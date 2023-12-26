package com.bigdata.insightanalytics.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@NoArgsConstructor
@AllArgsConstructor
@Data
public class NameValue {
    private String name;
    private Object value;
}
