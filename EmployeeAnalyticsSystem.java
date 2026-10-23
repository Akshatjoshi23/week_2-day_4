
import java.util.*;
import java.util.concurrent.*;

class Employee {
    private final int id;
    private final String name;
    private final String department;
    private final double salary;

    public Employee(int id, String name,
                    String department, double salary) {
        this.id = id;
        this.name = name;
        this.department = department;
        this.salary = salary;
    }

    public int getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public String getDepartment() {
        return department;
    }

    public double getSalary() {
        return salary;
    }
}

class EmployeeReport {
    private final String name;
    private final double salary;
    private final double bonus;
    private final double total;

    public EmployeeReport(String name, double salary,
                          double bonus, double total) {
        this.name = name;
        this.salary = salary;
        this.bonus = bonus;
        this.total = total;
    }

    @Override
    public String toString() {
        return name
                + " | Salary: " + salary
                + " | Bonus: " + bonus
                + " | Total: " + total;
    }
}

public class EmployeeAnalyticsSystem {

    public static void main(String[] args) {

        ExecutorService executor =
                Executors.newFixedThreadPool(3);

        ConcurrentHashMap<String, Integer> departmentCounts =
                new ConcurrentHashMap<>();

        List<Employee> employees = List.of(
            new Employee(101, "Amit", "IT", 30000),
            new Employee(102, "Rahul", "HR", 40000),
            new Employee(103, "Priya", "IT", 50000),
            new Employee(104, "Neha", "Finance", 35000),
            new Employee(105, "Riya", "HR", 45000)
        );

        List<CompletableFuture<EmployeeReport>> futures =
                new ArrayList<>();

        try {
            for (Employee employee : employees) {

                CompletableFuture<EmployeeReport> future =
                    CompletableFuture.supplyAsync(() -> {

                        System.out.println(
                            "Processing " + employee.getName()
                            + " on "
                            + Thread.currentThread().getName()
                        );

                        double salary = employee.getSalary();

                        departmentCounts.merge(
                            employee.getDepartment(),
                            1,
                            Integer::sum
                        );

                        return salary;

                    }, executor).thenCombine(

                        CompletableFuture.supplyAsync(() ->
                            employee.getSalary() * 0.10,
                            executor
                        ),

                        (salary, bonus) ->
                            new EmployeeReport(
                                employee.getName(),
                                salary,
                                bonus,
                                salary + bonus
                            )
                    );

                futures.add(future);
            }

            CompletableFuture<Void> allTasks =
                CompletableFuture.allOf(
                    futures.toArray(new CompletableFuture[0])
                );

            // Wait until all reports have completed.
            allTasks.join();

            System.out.println("\n=== EMPLOYEE REPORTS ===");

            for (CompletableFuture<EmployeeReport> future : futures) {
                System.out.println(future.join());
            }

            System.out.println("\n=== DEPARTMENT COUNTS ===");

            departmentCounts.forEach((department, count) ->
                System.out.println(department + ": " + count)
            );

        } catch (CompletionException e) {
            System.out.println(
                "Processing failed: " + e.getCause()
            );

        } finally {
            executor.shutdown();

            try {
                if (!executor.awaitTermination(
                        5, TimeUnit.SECONDS)) {
                    executor.shutdownNow();
                }
            } catch (InterruptedException e) {
                executor.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
    }
}
