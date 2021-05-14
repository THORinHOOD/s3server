package com.thorinhood;

import com.thorinhood.utils.ArgumentParser;
import junit.framework.TestCase;
import org.junit.Assert;

import java.util.Map;
import java.util.Set;

public class ArgumentParserTest extends TestCase {

    public void testParseArguments() {
        Map<String, String> result = ArgumentParser.parseArguments(new String[]{
                "--dbUser=xxx",
                "--param=yyy",
                "--yyet=zzz"
        });

        Map<String, String> expected = Map.of(
            "dbUser", "xxx",
            "param", "yyy",
            "yyet", "zzz"
        );

        Assert.assertEquals(result.size(), 3);
        for (String key : expected.keySet()) {
            Assert.assertTrue(result.containsKey(key));
            Assert.assertEquals(result.get(key), expected.get(key));
        }
    }

    public void testCheckArguments() {
        Set<String> result = ArgumentParser.checkArguments(Map.of(
                "first", "1",
                "second", "2"
        ), "first", "third", "second");
        Assert.assertEquals(result.size(), 1);
        Assert.assertEquals(result.iterator().next(), "third");
    }

}