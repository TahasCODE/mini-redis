package com.example.miniredis.store;


final class GlobMatcher {

    private GlobMatcher() {
    }

    static boolean matches(String pattern, String text) {
        return match(pattern, 0, text, 0);
    }

    private static boolean match(String p, int pi, String s, int si) {
        while (pi < p.length()) {
            char c = p.charAt(pi);
            if (c == '*') {
                while (pi + 1 < p.length() && p.charAt(pi + 1) == '*') {
                    pi++;                                   // "**" is the same as "*"
                }
                if (pi + 1 == p.length()) {
                    return true;                            // a trailing * matches everything left
                }
                for (int start = si; start <= s.length(); start++) {   // try every possible length for the *
                    if (match(p, pi + 1, s, start)) {
                        return true;
                    }
                }
                return false;
            }
            if (si >= s.length()) {
                return false;                               // pattern wants a character, text has none
            }
            char sc = s.charAt(si);
            if (c == '?') {
                pi++;
            } else if (c == '[') {
                pi++;
                boolean negate = pi < p.length() && p.charAt(pi) == '^';
                if (negate) {
                    pi++;
                }
                boolean found = false;
                while (pi < p.length() && p.charAt(pi) != ']') {
                    char pc = p.charAt(pi);
                    if (pc == '\\' && pi + 1 < p.length()) {
                        pi++;
                        found |= p.charAt(pi) == sc;
                    } else if (pi + 2 < p.length() && p.charAt(pi + 1) == '-') {
                        char low = (char) Math.min(pc, p.charAt(pi + 2));
                        char high = (char) Math.max(pc, p.charAt(pi + 2));
                        found |= sc >= low && sc <= high;
                        pi += 2;
                    } else {
                        found |= pc == sc;
                    }
                    pi++;
                }
                if (pi < p.length()) {
                    pi++;                                   // skip the closing ]
                }
                if (found == negate) {
                    return false;
                }
            } else {
                if (c == '\\' && pi + 1 < p.length()) {
                    pi++;
                    c = p.charAt(pi);
                }
                if (c != sc) {
                    return false;
                }
                pi++;
            }
            si++;
        }
        return si == s.length();
    }
}