package utils;

import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class ArgumentParser {
    public static Map<String, String> parseArguments(String[] args) {
        return Arrays.stream(args)
                .map(arg -> {
                    String[] splitted = arg.split("=");
                    splitted[0] = splitted[0].substring(2);
                    return splitted;
                })
                .collect(Collectors.toMap(
                        pair -> pair[0],
                        pair -> pair[1]
                ));
    }

    public static Set<String> checkArguments(Map<String, String> parsedArgs, String... keys) {
        return Arrays.stream(keys)
                .filter(key -> !parsedArgs.containsKey(key))
                .collect(Collectors.toSet());
    }
}
