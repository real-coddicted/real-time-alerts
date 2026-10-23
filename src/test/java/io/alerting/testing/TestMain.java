package io.alerting.testing;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/** Minimal JUnit-free runner. Usage: {@code TestMain [substring-filter]}. */
public final class TestMain {
    private static final List<Class<?>> SUITE = List.of(
            io.alerting.config.JsonTest.class,
            io.alerting.rules.ThresholdTest.class,
            io.alerting.rules.RuleEvaluatorTest.class,
            io.alerting.rules.RuleCatalogTest.class,
            io.alerting.engine.AlertIdGeneratorTest.class,
            io.alerting.config.ConfigStreamsTest.class,
            io.alerting.engine.StateMachineTest.class,
            io.alerting.engine.RiskScenarioTest.class);

    public static void main(String[] args) throws Exception {
        Logger.getLogger("").setLevel(Level.OFF);
        String filter = args.length > 0 ? args[0] : "";
        int passed = 0;
        List<String> failures = new ArrayList<>();
        for (Class<?> c : SUITE) {
            Method[] methods = c.getDeclaredMethods();
            Arrays.sort(methods, Comparator.comparing(Method::getName));
            for (Method m : methods) {
                if (!m.isAnnotationPresent(Test.class)) continue;
                String name = c.getSimpleName() + "." + m.getName();
                if (!name.contains(filter)) continue;
                try {
                    Object instance = c.getDeclaredConstructor().newInstance();
                    m.setAccessible(true);
                    m.invoke(instance);
                    passed++;
                    System.out.println("PASS " + name);
                } catch (InvocationTargetException e) {
                    Throwable t = e.getCause();
                    failures.add(name + ": " + t);
                    System.out.println("FAIL " + name + ": " + t);
                    t.printStackTrace(System.out);
                }
            }
        }
        System.out.println();
        System.out.println(passed + " passed, " + failures.size() + " failed");
        if (!failures.isEmpty()) {
            failures.forEach(f -> System.out.println("  " + f));
            System.exit(1);
        }
    }
}
