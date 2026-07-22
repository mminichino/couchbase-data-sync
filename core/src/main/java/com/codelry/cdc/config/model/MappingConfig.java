package com.codelry.cdc.config.model;

import java.util.ArrayList;
import java.util.List;

/**
 * Root model for mapping.yaml.
 */
public class MappingConfig {

    private List<TableMapping> mappings = new ArrayList<>();

    public List<TableMapping> getMappings() {
        return mappings;
    }

    public void setMappings(List<TableMapping> mappings) {
        this.mappings = mappings != null ? mappings : new ArrayList<>();
    }

    public static class TableMapping {
        private String source;
        private TargetMapping target = new TargetMapping();
        private KeyMapping key = new KeyMapping();
        private DocumentMapping document = new DocumentMapping();
        private boolean softDelete;

        public String getSource() {
            return source;
        }

        public void setSource(String source) {
            this.source = source;
        }

        public TargetMapping getTarget() {
            return target;
        }

        public void setTarget(TargetMapping target) {
            this.target = target;
        }

        public KeyMapping getKey() {
            return key;
        }

        public void setKey(KeyMapping key) {
            this.key = key;
        }

        public DocumentMapping getDocument() {
            return document;
        }

        public void setDocument(DocumentMapping document) {
            this.document = document;
        }

        public boolean isSoftDelete() {
            return softDelete;
        }

        public void setSoftDelete(boolean softDelete) {
            this.softDelete = softDelete;
        }
    }

    public static class TargetMapping {
        private String scope;
        private String collection;

        public String getScope() {
            return scope;
        }

        public void setScope(String scope) {
            this.scope = scope;
        }

        public String getCollection() {
            return collection;
        }

        public void setCollection(String collection) {
            this.collection = collection;
        }
    }

    public static class KeyMapping {
        /**
         * Template referencing source columns, e.g. {@code order::{ID}},
         * or special tokens {@code {pk}} / {@code {uuid4}}.
         */
        private String template;

        public String getTemplate() {
            return template;
        }

        public void setTemplate(String template) {
            this.template = template;
        }
    }

    public static class DocumentMapping {
        private List<FieldMapping> fields = new ArrayList<>();
        private List<String> omit = new ArrayList<>();

        public List<FieldMapping> getFields() {
            return fields;
        }

        public void setFields(List<FieldMapping> fields) {
            this.fields = fields != null ? fields : new ArrayList<>();
        }

        public List<String> getOmit() {
            return omit;
        }

        public void setOmit(List<String> omit) {
            this.omit = omit != null ? omit : new ArrayList<>();
        }
    }

    public static class FieldMapping {
        private String source;
        private String target;
        /** iso8601 | string | number | boolean | epochMillis (optional) */
        private String type;

        public String getSource() {
            return source;
        }

        public void setSource(String source) {
            this.source = source;
        }

        public String getTarget() {
            return target;
        }

        public void setTarget(String target) {
            this.target = target;
        }

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }
    }
}
