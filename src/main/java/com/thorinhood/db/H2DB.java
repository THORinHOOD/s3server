package com.thorinhood.db;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;

public class H2DB {

    private final static String TYPE = "jdbc:h2:file:";
    private final static String KEY = "META_KEY";
    private final static String VALUE = "META_VALUE";
    private final static String KEY_FILE = "KEY_FILE";

    private final static String QUERY_CREATE_TABLE = String.format("CREATE TABLE IF NOT EXISTS META (%s VARCHAR, " +
            "%s VARCHAR, %s VARCHAR, PRIMARY KEY(%s, %s))", KEY_FILE, KEY, VALUE, KEY_FILE, KEY);
    private final static String INSERT = String.format("INSERT INTO META (%s, %s, %s) " +
            "VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE %s = VALUES(%s)", KEY_FILE, KEY, VALUE, VALUE, VALUE);
    private final static String SELECT_ALL_BY_FILE = String.format("SELECT * FROM META WHERE %s = ?", KEY_FILE);

    private static H2DB h2db;
    private final Connection conn;

    private H2DB(Connection conn) {
        this.conn = conn;
    }

    public static H2DB getInstance(String url, String user, String password) throws SQLException {
        if (h2db == null) {
            Connection conn = DriverManager.getConnection(TYPE + url + ";mode=MySQL", user, password);
            h2db = new H2DB(conn);
        }
        return h2db;
    }

    public boolean init() throws SQLException {
        Statement st = conn.createStatement();
        return st.execute(QUERY_CREATE_TABLE);
    }

    public void setFileMetadata(String keyFile, Map<String, String> metadata) throws SQLException {
        for (String key : metadata.keySet()) {
            String value = metadata.get(key);
            PreparedStatement pst = conn.prepareStatement(INSERT);
            pst.setString(1, keyFile);
            pst.setString(2, key);
            pst.setString(3, value);
            pst.execute();
        }
    }

    public Map<String, String> getFileMetadata(String keyFile) throws SQLException {
        PreparedStatement pst = conn.prepareStatement(SELECT_ALL_BY_FILE);
        pst.setString(1, keyFile);
        Map<String, String> metadata = new HashMap<>();
        try (ResultSet rs = pst.executeQuery()) {
            while (rs.next()) {
                String key = rs.getString(KEY);
                String value = rs.getString(VALUE);
                metadata.put(key, value);
            }
        }
        return metadata;
    }

}
