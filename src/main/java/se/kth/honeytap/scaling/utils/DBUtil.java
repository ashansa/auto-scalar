package se.kth.honeytap.scaling.utils;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import se.kth.honeytap.scaling.exceptions.DBConnectionFailureException;

import java.io.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

/**
 * Created with IntelliJ IDEA.
 *
 * @author Ashansa Perera
 * @version $Id$
 * @since 1.0
 */
public class DBUtil {

    private static final Log log = LogFactory.getLog(DBUtil.class);

    //TODO copy the properties file to outside and use only the dbConnection.properties as the below value
    private static final String DB_PROPERTY_FILE = "/karamel-core/src/main/resources/dbConnection.properties";

    public static Connection getDBConnection() throws DBConnectionFailureException {
        Properties prop = new Properties();
        InputStream in = null;
        String propertyFilePath = DB_PROPERTY_FILE;
        try {
            propertyFilePath =  getPropertyFilePath();
            in = new FileInputStream(new File(propertyFilePath));
            prop.load(in);
            in.close();

        } catch (FileNotFoundException e) {
            DBConnectionFailureException exception = handleDBConnectionException(e.getMessage(), e);
            throw exception;
        } catch (IOException e) {
            DBConnectionFailureException exception = handleDBConnectionException("Error occurred while reading " +
                    propertyFilePath, e);
            throw exception;
        }

        try {
            Class.forName("com.mysql.jdbc.Driver");

            String urlHostPort = prop.getProperty("db.urlhostport");
            String dbName = prop.getProperty("db.name");
            String username = prop.getProperty("db.username");
            String password = prop.getProperty("db.password");
            String connectionURL;

            if (urlHostPort.endsWith("/"))
                connectionURL = urlHostPort.concat(dbName);
            else
                connectionURL = urlHostPort.concat("/").concat(dbName);

            Connection connection = DriverManager.getConnection(connectionURL, username, password);
            log.info("DB connection successful to " + dbName + " with user " + username);
            return connection;
        } catch (SQLException e) {
            e.printStackTrace();
            DBConnectionFailureException exception = handleDBConnectionException(
                    "Error occurred while connecting to database.", e);
            throw exception;
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            DBConnectionFailureException exception = handleDBConnectionException("DB driver class not found", e);
            throw exception;
        }
    }

    public static Connection getInMemoryDBConnection() throws DBConnectionFailureException {
        Properties prop = new Properties();
        InputStream in = null;
        String propertyFilePath = DB_PROPERTY_FILE;
        try {
            propertyFilePath =  getPropertyFilePath();
            in = new FileInputStream(new File(propertyFilePath));
            prop.load(in);
            in.close();

        } catch (FileNotFoundException e) {
            DBConnectionFailureException exception = handleDBConnectionException(e.getMessage(), e);
            throw exception;
        } catch (IOException e) {
            DBConnectionFailureException exception = handleDBConnectionException("Error occurred while reading " +
                    propertyFilePath, e);
            throw exception;
        }

        try {

            Class.forName("org.hsqldb.jdbcDriver");
            String dbName = prop.getProperty("db.name");
            Connection connection =  DriverManager.getConnection("jdbc:hsqldb:mem:" + dbName, "SA", "");  //creates ruleDB if not exists

            log.info("DB connection successful to " + dbName + " with user SA");
            return connection;
        } catch (SQLException e) {
            e.printStackTrace();
            DBConnectionFailureException exception = handleDBConnectionException(
                    "Error occurred while connecting to database.", e);
            throw exception;
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            DBConnectionFailureException exception = handleDBConnectionException("DB driver class not found", e);
            throw exception;
        }
    }

    public static Connection getKandyDbConnection() throws DBConnectionFailureException {
        Properties prop = new Properties();
        InputStream in = null;
        String propertyFilePath = DB_PROPERTY_FILE;
        try {
            propertyFilePath =  getPropertyFilePath();
            in = new FileInputStream(new File(propertyFilePath));
            prop.load(in);
            in.close();

        } catch (FileNotFoundException e) {
            DBConnectionFailureException exception = handleDBConnectionException(e.getMessage(), e);
            throw exception;
        } catch (IOException e) {
            DBConnectionFailureException exception = handleDBConnectionException("Error occurred while reading " +
                    propertyFilePath, e);
            throw exception;
        }

        try {
            Class.forName("com.mysql.jdbc.Driver");

            String urlHostPort = prop.getProperty("kandydb.urlhostport");
            String dbName = prop.getProperty("kandydb.name");
            String username = prop.getProperty("kandydb.username");
            String password = prop.getProperty("kandydb.password");
            String connectionURL;

            if (urlHostPort.endsWith("/"))
                connectionURL = urlHostPort.concat(dbName);
            else
                connectionURL = urlHostPort.concat("/").concat(dbName);

            Connection connection = DriverManager.getConnection(connectionURL, username, password);
            log.info("DB connection successful to " + dbName + " with user " + username);
            return connection;
        } catch (SQLException e) {
            e.printStackTrace();
            DBConnectionFailureException exception = handleDBConnectionException(
                    "Error occurred while connecting to database.", e);
            throw exception;
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            DBConnectionFailureException exception = handleDBConnectionException("DB driver class not found", e);
            throw exception;
        }
    }

    public static DBConnectionFailureException handleDBConnectionException(String msg, Exception e) {
        log.error(msg);
        e.printStackTrace();
        return new DBConnectionFailureException(msg, e.getCause());
    }

    private static String getPropertyFilePath() throws IOException {
        return new File(".").getCanonicalPath().concat(DB_PROPERTY_FILE);
    }
}
