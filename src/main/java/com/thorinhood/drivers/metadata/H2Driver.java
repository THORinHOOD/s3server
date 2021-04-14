package com.thorinhood.drivers.metadata;

import com.thorinhood.exceptions.S3Exception;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;

public class H2Driver implements MetadataDriver {

    private final static String TYPE = "jdbc:h2:file:";
    private final static String KEY = "META_KEY";
    private final static String VALUE = "META_VALUE";
    private final static String KEY_FILE = "KEY_FILE";

    private final static String QUERY_CREATE_TABLE = String.format("CREATE TABLE IF NOT EXISTS META (%s VARCHAR, " +
            "%s VARCHAR, %s VARCHAR, PRIMARY KEY(%s, %s))", KEY_FILE, KEY, VALUE, KEY_FILE, KEY);
    private final static String INSERT = String.format("INSERT INTO META (%s, %s, %s) " +
            "VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE %s = VALUES(%s)", KEY_FILE, KEY, VALUE, VALUE, VALUE);
    private final static String SELECT_ALL_BY_FILE = String.format("SELECT * FROM META WHERE %s = ?", KEY_FILE);
    private final static String DELETE = String.format("DELETE FROM META WHERE %s = ?", KEY_FILE);

    private static H2Driver h2Driver;
    private final Connection conn;

    private H2Driver(Connection conn) {
        this.conn = conn;
    }

    public static H2Driver getInstance(String url, String user, String password) throws SQLException {
        if (h2Driver == null) {
            Connection conn = DriverManager.getConnection(TYPE + url + ";mode=MySQL", user, password);
            h2Driver = new H2Driver(conn);
        }
        return h2Driver;
    }

    public boolean init() throws Exception {
        Statement st = conn.createStatement();
        return st.execute(QUERY_CREATE_TABLE);
    }

    public void setObjectMetadata(String keyFile, Map<String, String> metadata) throws S3Exception {
        try {
            PreparedStatement pstDlt = conn.prepareStatement(DELETE);
            pstDlt.setString(1, keyFile);
            pstDlt.execute();
            for (String key : metadata.keySet()) {
                String value = metadata.get(key);
                PreparedStatement pst = conn.prepareStatement(INSERT);
                pst.setString(1, keyFile);
                pst.setString(2, key);
                pst.setString(3, value);
                pst.execute();
            }
        } catch (SQLException exception) {
            throw S3Exception.INTERNAL_ERROR(exception.getMessage())
                    .setMessage(exception.getMessage())
                    .setResource("1")
                    .setRequestId("1");
        }
    }

    public Map<String, String> getObjectMetadata(String key) throws S3Exception {
        try {
            PreparedStatement pst = conn.prepareStatement(SELECT_ALL_BY_FILE);
            pst.setString(1, key);
            Map<String, String> metadata = new HashMap<>();
            try (ResultSet rs = pst.executeQuery()) {
                while (rs.next()) {
                    String keyMeta = rs.getString(KEY);
                    String valueMeta = rs.getString(VALUE);
                    metadata.put(keyMeta, valueMeta);
                }
            }
            return metadata;
        } catch (SQLException exception) {
            throw S3Exception.INTERNAL_ERROR(exception.getMessage())
                    .setMessage(exception.getMessage())
                    .setResource("1")
                    .setRequestId("1");
        }
    }

}
