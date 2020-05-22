package com.generic.client.hdfs.models;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class HiveSchema {


        private String table_name;
        private String location;
        @JsonProperty("columns")
        private List<HiveColumns>  columns;

        public List<HiveColumns> getColumns() {
            return columns;
        }

        public void setColumns(List<HiveColumns>  columns) {
            this.columns = columns;
        }

        public String getTable_name() {
            return table_name;
        }

        public void setTable_name(String table_name) {
            this.table_name = table_name;
        }

        public String getLocation() {
            return location;
        }

        public void setLocation(String location) {
            this.location = location;
        }

}
