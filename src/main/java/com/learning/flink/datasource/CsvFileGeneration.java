package com.learning.flink.datasource;

import com.opencsv.CSVWriter;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

public class CsvFileGeneration implements Runnable {

    private static final String ANSI_BLUE = "\033[0;34m";
    private static final String ANSI_RED = "\033[0;31m";

    public static void main(String[] args) {
        CsvFileGeneration csvFileGeneration = new CsvFileGeneration();
        csvFileGeneration.run();
    }

    @Override
    public void run() {
        try {
            // Define list of users
            List<String> appUser = new ArrayList<>();
            appUser.add("Hamza");
            appUser.add("Maaz");
            appUser.add("Talha");
            appUser.add("Shaaz");

            // Define list of application operations
            List<String> appOps = new ArrayList<>();
            appOps.add("Create");
            appOps.add("Read");
            appOps.add("Update");
            appOps.add("Delete");

            // Define list of application entities
            List<String> appEntity = new ArrayList<>();
            appEntity.add("Customer");
            appEntity.add("SalesRep");

            // Define the directory to output csv files
            String dir = "data/raw_audit_trail";

            // Clean out existing files in the directory
            FileUtils.cleanDirectory(new File(dir));

            // Define a random number generator
            Random random = new Random();

            // Generate 100 random records 1 per each file
            for (int i=0; i<100; i++) {

                // Get current_timestamp in String
                String timestamp = String.valueOf(System.currentTimeMillis());
                // Generate a random user
                String user = appUser.get(random.nextInt(appUser.size()));
                // Generate a random operation
                String operation = appOps.get(random.nextInt(appOps.size()));
                // Generate a random entity
                String entity = appEntity.get(random.nextInt(appEntity.size()));
                // Generate a random duration for the operation
                String duration = String.valueOf(random.nextInt(10) + 1);
                // Generate a random value for number of changes
                String changeCount = String.valueOf(random.nextInt(4) + 1);

                // Create a CSV text array
                String[] csvText = {String.valueOf(i), user, entity, operation, timestamp, duration, changeCount};

                // Open a file for this record
                FileWriter auditFile = new FileWriter(dir + "/audit_file_"+i+".csv");
                CSVWriter auditCsv = new CSVWriter(auditFile);

                // Write the audit record
                auditCsv.writeNext(csvText);

                System.out.println(ANSI_BLUE + "CsvFileGeneration : Creating File : " + Arrays.toString(csvText) + ANSI_RED);

                // Close the file
                auditFile.flush();
                auditFile.close();

                // Sleep for a random time (1 - 3 seconds) before the next record
                Thread.sleep(random.nextInt(2000) + 1);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}





























