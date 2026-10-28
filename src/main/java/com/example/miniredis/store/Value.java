package com.example.miniredis.store;


sealed interface Value permits StringValue, ListValue {
}