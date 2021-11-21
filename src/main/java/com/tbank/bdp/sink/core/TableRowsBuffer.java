package com.tbank.bdp.sink.core;

import java.util.ArrayList;
import java.util.List;

/**
 * @author leiline
 * @date 2021/11/20
 */
public class TableRowsBuffer {

    private final String table;
    private final List<String> rows;
    private final List<String> fields;


    public TableRowsBuffer(String table) {
        this.table = table;
        this.rows = new ArrayList<>();
        this.fields = new ArrayList<>();
    }

    public void add(String row, String field) {
        rows.add(row);
        fields.add(field);
    }

    public int bufferSize() {
        return rows.size();
    }

    public List<String> getRows() {
        return rows;
    }

    public String getTable() {
        return table;
    }

    public List<String> getFields() { return fields; }
}
