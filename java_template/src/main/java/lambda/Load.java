package lambda;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import saaf.Inspector;
import saaf.Response;

import java.io.*;
import java.sql.*;
import java.util.HashMap;

public class Load implements RequestHandler<HashMap<String, Object>, HashMap<String, Object>> {

    /**
     * Lambda Function Handler
     * 
     * @param request Hashmap containing request JSON attributes.
     * @param context 
     * @return HashMap that Lambda will automatically convert into JSON.
     */
    public HashMap<String, Object> handleRequest(HashMap<String, Object> request, Context context) {
        
         // Collect initial data
        Inspector inspector = new Inspector();
        inspector.inspectAll();

        //****************START FUNCTION IMPLEMENTATION*************************
        Response response = new Response();
        try {
            // Input parameters
            String bucket = (String) request.get("bucket");
            String key = (String) request.get("key");
            String dbType = (String) request.getOrDefault("dbType", "sqlite"); // Default to SQLite

            if (bucket == null || key == null) {
                throw new IllegalArgumentException("S3 bucket and key must be provided.");
            }

            // Step 1: Download CSV file from S3
            File csvFile = downloadCSVFromS3(bucket, key);

            // Step 2: Load data into a relational database
            if ("aurora".equalsIgnoreCase(dbType)) {
                loadIntoAurora(csvFile);
                response.setValue("Data successfully loaded into Aurora database.");
            } else if ("sqlite".equalsIgnoreCase(dbType)) {
                loadIntoSQLite(csvFile);
                response.setValue("Data successfully loaded into SQLite database.");
            } else {
                throw new IllegalArgumentException("Unsupported database type: " + dbType);
            }

            // Add success message to inspector
            inspector.addAttribute("status", "success");
        } catch (Exception e) {
            // Capture any errors
            inspector.addAttribute("error", e.getMessage());
            response.setValue("Error: " + e.getMessage());
        }

        // Add the response to the inspector
        inspector.consumeResponse(response);

        //****************END FUNCTION IMPLEMENTATION***************************

        // Collect final information such as total runtime and CPU deltas
        inspector.inspectAllDeltas();
        return inspector.finish();
    }
     private File downloadCSVFromS3(String bucket, String key) throws IOException {
        AmazonS3 s3Client = AmazonS3ClientBuilder.defaultClient();
        File tempFile = File.createTempFile("transformed-data", ".csv");
        s3Client.getObject(bucket, key).getObjectContent().transferTo(new FileOutputStream(tempFile));
        return tempFile;
    }

    private void loadIntoSQLite(File csvFile) throws SQLException, IOException {
        // SQLite Database URL
        String jdbcUrl = "jdbc:sqlite:transformed_data.db";

        try (Connection conn = DriverManager.getConnection(jdbcUrl)) {
            createTableIfNotExists(conn);

            // Load CSV data
            try (BufferedReader reader = new BufferedReader(new FileReader(csvFile))) {
                String line;
                boolean isHeader = true;

                while ((line = reader.readLine()) != null) {
                    if (isHeader) {
                        isHeader = false; // Skip header row
                        continue;
                    }
                    String[] columns = line.split(",");
                    insertRow(conn, columns);
                }
            }
        }
    }

    private void loadIntoAurora(File csvFile) throws SQLException, IOException {
        // Aurora Database URL (replace placeholders with actual values)
        String jdbcUrl = "jdbc:postgresql://<AURORA_ENDPOINT>:5432/<DATABASE_NAME>";
        String username = "<USERNAME>";
        String password = "<PASSWORD>";

        try (Connection conn = DriverManager.getConnection(jdbcUrl, username, password)) {
            createTableIfNotExists(conn);

            // Load CSV data
            try (BufferedReader reader = new BufferedReader(new FileReader(csvFile))) {
                String line;
                boolean isHeader = true;

                while ((line = reader.readLine()) != null) {
                    if (isHeader) {
                        isHeader = false; // Skip header row
                        continue;
                    }
                    String[] columns = line.split(",");
                    insertRow(conn, columns);
                }
            }
        }
    }

    private void createTableIfNotExists(Connection conn) throws SQLException {
        String createTableSQL = """
            CREATE TABLE IF NOT EXISTS transformed_data (
                Order_ID VARCHAR(255) PRIMARY KEY,
                Region VARCHAR(255),
                Country VARCHAR(255),
                Item_Type VARCHAR(255),
                Sales_Channel VARCHAR(255),
                Order_Priority VARCHAR(255),
                Order_Date DATE,
                Ship_Date DATE,
                Units_Sold INTEGER,
                Unit_Price FLOAT,
                Unit_Cost FLOAT,
                Total_Revenue FLOAT,
                Order_Processing_Time INTEGER,
                Gross_Margin FLOAT
            )
            """;
        try (Statement stmt = conn.createStatement()) {
            stmt.execute(createTableSQL);
        }
    }

    private void insertRow(Connection conn, String[] columns) throws SQLException {
        String insertSQL = """
            INSERT INTO transformed_data (
                Order_ID, Region, Country, Item_Type, Sales_Channel, Order_Priority,
                Order_Date, Ship_Date, Units_Sold, Unit_Price, Unit_Cost,
                Total_Revenue, Order_Processing_Time, Gross_Margin
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """;
        try (PreparedStatement pstmt = conn.prepareStatement(insertSQL)) {
            pstmt.setString(1, columns[5]); // Order ID
            pstmt.setString(2, columns[0]); // Region
            pstmt.setString(3, columns[1]); // Country
            pstmt.setString(4, columns[2]); // Item Type
            pstmt.setString(5, columns[3]); // Sales Channel
            pstmt.setString(6, columns[4]); // Order Priority
            pstmt.setDate(7, java.sql.Date.valueOf(columns[6])); // Order Date
            pstmt.setDate(8, java.sql.Date.valueOf(columns[7])); // Ship Date
            pstmt.setInt(9, Integer.parseInt(columns[8])); // Units Sold
            pstmt.setFloat(10, Float.parseFloat(columns[9])); // Unit Price
            pstmt.setFloat(11, Float.parseFloat(columns[10])); // Unit Cost
            pstmt.setFloat(12, Float.parseFloat(columns[11])); // Total Revenue
            pstmt.setInt(13, Integer.parseInt(columns[12])); // Order Processing Time
            pstmt.setFloat(14, Float.parseFloat(columns[13])); // Gross Margin
            pstmt.executeUpdate();
        }
    }
}