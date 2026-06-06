package com.example.downstream.model;

import com.fasterxml.jackson.annotation.JsonInclude;

@JsonInclude(JsonInclude.Include.NON_NULL)
public record RequestMessage(String id, String destination, String payload) {}
