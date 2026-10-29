package com.example.miniredis.store;

import java.util.LinkedHashMap;


final class HashValue implements Value {

    final LinkedHashMap<String, String> fields = new LinkedHashMap<>();
}