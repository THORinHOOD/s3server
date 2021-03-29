package com.thorinhood.db;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;

public class H2DB {

    private final static String TYPE = "jdbc:h2:file:";

    private static H2DB h2db;
    private final Connection conn;

    private H2DB(Connection conn) {
        this.conn = conn;
    }

    public static H2DB getInstance(String url, String user, String password) throws SQLException {
        if (h2db == null) {
            Connection conn = DriverManager.getConnection(TYPE + url, user, password);
            h2db = new H2DB(conn);
        }
        return h2db;
    }

    public boolean init() throws SQLException {
        Statement st = conn.createStatement();
        return st.execute("CREATE TABLE IF NOT EXISTS META (KEY_FILE VARCHAR, KEY VARCHAR, VALUE VARCHAR, " +
                "PRIMARY KEY(KEY_FILE, KEY))");
    }

    public void setFileMetadata(String keyFile, Map<String, String> metadata) throws SQLException {
        Statement st = conn.createStatement();
        for (String key : metadata.keySet()) {
            String value = metadata.get(key);
            PreparedStatement pst = conn.prepareStatement("INSERT INTO META(KEY_FILE, KEY, VALUE) VALUE(?, ?, ?) " +
                    "ON DUPLICATE KEY UPDATE VALUE = ?");
            pst.setString(1, keyFile);
            pst.setString(2, key);
            pst.setString(3, value);
            pst.setString(4, value);
            pst.execute();
        }
    }

    public Map<String, String> getFileMetadata(String keyFile) throws SQLException {
        Statement st = conn.createStatement();
        PreparedStatement pst = conn.prepareStatement("SELECT * FROM META WHERE KEY_FILE = ?");
        pst.setString(1, keyFile);
        Map<String, String> metadata = new HashMap<>();
        try (ResultSet rs = pst.executeQuery()) {
            while (rs.next()) {
                String key = rs.getString("KEY");
                String value = rs.getString("VALUE");
                metadata.put(key, value);
            }
        }
        return metadata;
    }

}
