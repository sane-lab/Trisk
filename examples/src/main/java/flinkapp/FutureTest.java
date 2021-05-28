package flinkapp;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static java.lang.Thread.sleep;
import static java.util.concurrent.CompletableFuture.runAsync;

public class FutureTest {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
//        CompletableFuture futrue = new CompletableFuture();
//        String expectedValue = "the expected value";
//        CompletableFuture<String> alreadyCompleted = CompletableFuture.completedFuture(expectedValue);
//        assert (alreadyCompleted.get().equals(expectedValue));
//        System.out.println(alreadyCompleted.get());

//        System.out.printf("[%s] I am Cool\n", Thread.currentThread().getName());
//        CompletableFuture<Void> cf = CompletableFuture.runAsync(() -> {
//            System.out.printf("[%s] I am Cool\n", Thread.currentThread().getName());
//        });
//
//        CompletableFuture<String> cf1 = CompletableFuture.supplyAsync(() -> {
//            long start = System.currentTimeMillis();
//            while(System.currentTimeMillis() - start < 100) {}
//            System.out.printf("[%s] Am Awesome\n", Thread.currentThread().getName());
//            return null;
//        });
//        cf.get();

//        while (true) {
//            {
//                CompletableFuture cf = CompletableFuture.supplyAsync(() -> {
//                    try {
//                        Thread.sleep(1000);
//                    } catch (InterruptedException e) {
//                        e.printStackTrace();
//                    }
//                    System.out.println();
//                    return "I am Cool";
//                }).thenAccept(msg ->
//                        System.out.printf("[%s] %s and am also Awesome\n", Thread.currentThread().getName(), msg));
//                try {
//                    cf.get();
//                } catch (Exception ex) {
//                    ex.printStackTrace(System.err);
//                }
//            }
//        }
        int operatorIndex = 9;
        int maxParallelism = 128;
        int parallelism = 10;

        for (operatorIndex=0; operatorIndex < 10; operatorIndex++) {
            int start = ((operatorIndex * maxParallelism + parallelism - 1) / parallelism);
            int end = ((operatorIndex + 1) * maxParallelism - 1) / parallelism;
            System.out.println("start: " + start + ", end: " + end + ", nNumbers: " + (end - start + 1));
        }
    }
}
