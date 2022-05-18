package com.itmo.java.basics.config;

public class DatabaseConfig {
    public static final String DEFAULT_WORKING_PATH = "db_files";
    private String workingPath;

    public DatabaseConfig(String workingPath) {
        if (workingPath.isBlank()) {
            String projectDirectory = System.getProperty("user.dir");
            this.workingPath = projectDirectory + DatabaseConfig.DEFAULT_WORKING_PATH;
        } else {
            this.workingPath = workingPath;
        }
    }

    public String getWorkingPath() {
        return workingPath;
    }
}
