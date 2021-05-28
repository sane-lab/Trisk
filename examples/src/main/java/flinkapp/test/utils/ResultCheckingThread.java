package flinkapp.test.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class ResultCheckingThread extends Thread {

    private static final Logger logger = LoggerFactory.getLogger(ResultCheckingThread.class);

    public static void checkLogFileException(String logFilePath) throws Exception {
        File file = new File(logFilePath);
        InputStreamReader read = new InputStreamReader(new FileInputStream(file), StandardCharsets.UTF_8);
        try (BufferedReader bufferedReader = new BufferedReader(read)) {
            String line = null;
            int lineNumber = 0;
            while ((line = bufferedReader.readLine()) != null) {
                lineNumber++;
//            if(line.startsWith("#")){
//                continue;
//            }
                if (line.contains("Exception")) {
                    throw new CheckFailException("Exception record found in file:" +
                            line + "\n" + file.getAbsolutePath() + ":"+ lineNumber);
                }
            }
        }
    }

    public static ResultCheckingThread create(int delay, boolean checkAll) {
        ResultCheckingThread thread = new ResultCheckingThread(delay, checkAll);
        thread.setDaemon(false);
        return thread;
    }

    public interface CheckingFunction {
        void check() throws Exception;
    }

    private boolean checkAll;
    private int timeDelay;
    private List<CheckingFunction> functionList;

    private ResultCheckingThread(int delay, boolean checkAll) {
        super("file checking thread");
        functionList = new ArrayList<>();
        this.checkAll = checkAll;
        this.timeDelay = delay; // in seconds
    }

    public ResultCheckingThread addChecking(CheckingFunction function) {
        this.functionList.add(function);
        return this;
    }

    public void startWith(CheckingFunction function) throws InterruptedException {
        super.start();
        try {
            function.check();
        } catch (Exception e) {
            e.printStackTrace();
        }
        this.join();
    }

    @Override
    public synchronized void start() {
        super.start();
        try {
            this.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void run() {
        try {
            sleep(timeDelay * 1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        logger.info("start checking...");
        List<Exception> exceptions = new ArrayList<>(functionList.size());

        for (CheckingFunction function : functionList) {
            try {
                function.check();
            } catch (Exception e) {
                exceptions.add(e);
                if (!checkAll) {
                    break;
                }
            }
        }
        logger.info("Number of exceptions: " + exceptions.size());
        for (Exception e : exceptions) {
            e.printStackTrace();
        }
        logger.info("end checking...");
    }
}
