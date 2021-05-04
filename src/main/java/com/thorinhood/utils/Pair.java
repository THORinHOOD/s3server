package com.thorinhood.utils;

public class Pair<FIRST, SECOND> {
    private final FIRST first;
    private final SECOND second;

    public static <FIRST, SECOND> Pair<FIRST, SECOND> of(FIRST first, SECOND second) {
        return new Pair<>(first, second);
    }

    private Pair(FIRST first, SECOND second) {
        this.first = first;
        this.second = second;
    }

    public FIRST getFirst() {
        return first;
    }

    public SECOND getSecond() {
        return second;
    }
}
