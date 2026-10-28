package com.example.miniredis.store;

import java.util.ArrayDeque;


final class ListValue implements Value {

    final ArrayDeque<String> items = new ArrayDeque<>();
}