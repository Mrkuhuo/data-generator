package com.datagenerator.validator;

import lombok.Data;

import java.util.ArrayList;
import java.util.List;

@Data
public class ValidationResult {
    private boolean valid;
    private List<String> errors;

    public ValidationResult() {
        this.valid = true;
        this.errors = new ArrayList<>();
    }

    public void addError(String error) {
        this.valid = false;
        this.errors.add(error);
    }

    public void addErrors(List<String> errors) {
        this.valid = false;
        this.errors.addAll(errors);
    }
} 