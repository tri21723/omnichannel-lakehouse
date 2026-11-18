// JDBC check for DBeaver users (ADR-0015): same driver and URL as DBeaver; read-only, no secrets.
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

public class JdbcCheck {
    public static void main(String[] args) throws Exception {
        String url = args.length > 0 ? args[0] : "jdbc:trino://127.0.0.1:8083/iceberg/gold";
        Properties props = new Properties();
        props.setProperty("user", "jdbc-check");
        try (Connection c = DriverManager.getConnection(url, props);
             Statement s = c.createStatement()) {
            DatabaseMetaData md = c.getMetaData();
            System.out.println("driver " + md.getDriverVersion() + " server " + md.getDatabaseProductVersion());
            try (ResultSet rs = s.executeQuery(
                    "SELECT _dataset_run_id, count(*) FROM fact_orders GROUP BY 1 ORDER BY 1")) {
                while (rs.next()) {
                    System.out.println("orders " + rs.getString(1) + " " + rs.getLong(2));
                }
            }
            try (ResultSet rs = s.executeQuery("SELECT email FROM postgresql.walmart.customers LIMIT 1")) {
                rs.next();
                String email = rs.getString(1);
                if (!"[redacted]".equals(email)) {
                    throw new IllegalStateException("PII not masked");
                }
                System.out.println("email " + email);
            }
            try (PreparedStatement ps = c.prepareStatement(
                    "SELECT count(*) FROM fact_orders WHERE _dataset_run_id = ?")) {
                ps.setString(1, "5f552019ee41c24af95f");
                try (ResultSet rs = ps.executeQuery()) {
                    rs.next();
                    System.out.println("prepared " + rs.getLong(1));
                }
            }
            boolean denied = false;
            try {
                s.execute("DELETE FROM dim_channel WHERE false");
            } catch (SQLException e) {
                denied = e.getMessage().contains("Access Denied");
                System.out.println("delete denied: " + denied);
            }
            if (!denied) {
                throw new IllegalStateException("DELETE was not denied");
            }
        }
    }
}
