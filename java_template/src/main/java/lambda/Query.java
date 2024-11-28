package lambda;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import saaf.Inspector;
import saaf.Response;

import java.io.File;
import java.io.FileOutputStream;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class Query implements RequestHandler<HashMap<String, Object>, HashMap<String, Object>> {

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
            List<String> filters = (List<String>) request.getOrDefault("filters", new ArrayList<>());
            List<String> aggregations = (List<String>) request.getOrDefault("aggregations", new ArrayList<>());
            String groupByColumn = (String) request.get("groupBy");

            // Validate input
            if (groupByColumn == null || groupByColumn.isEmpty()) {
                throw new IllegalArgumentException("A 'groupBy' column must be specified.");
            }
            if (aggregations.isEmpty()) {
                throw new IllegalArgumentException("At least one aggregation function must be specified.");
            }

            // Step 1: Ensure the SQLite DB is available locally
            File dbFile = new File("transformed_data.db");
            if (!dbFile.exists()) {
                String bucket = (String) request.get("bucket");
                String key = (String) request.get("key");
                if (bucket == null || key == null) {
                    throw new IllegalArgumentException("S3 bucket and key must be provided to download the database.");
                }
                downloadDatabaseFromS3(bucket, key);
            }

            // Step 2: Execute the query with filters and aggregations
            List<HashMap<String, Object>> resultData = queryDatabase(filters, aggregations, groupByColumn);

            // Add results to the response
            response.setValue(resultData.toString());
            inspector.addAttribute("status", "success");
        } catch (Exception e) {
            // Handle errors
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
     private void downloadDatabaseFromS3(String bucket, String key) throws Exception {
        // Download the database file from S3
        AmazonS3 s3Client = AmazonS3ClientBuilder.defaultClient();
        File tempFile = new File("transformed_data.db");
        s3Client.getObject(bucket, key).getObjectContent().transferTo(new FileOutputStream(tempFile));
    }

    private List<HashMap<String, Object>> queryDatabase(List<String> filters, List<String> aggregations, String groupByColumn) throws SQLException {
        // SQLite Database URL
        String jdbcUrl = "jdbc:sqlite:transformed_data.db";
        List<HashMap<String, Object>> results = new ArrayList<>();

        try (Connection conn = DriverManager.getConnection(jdbcUrl)) {
            // Build the SQL query dynamically
            StringBuilder sql = new StringBuilder("SELECT ").append(groupByColumn);

            for (String aggregation : aggregations) {
                sql.append(", ").append(aggregation);
            }

            sql.append(" FROM transformed_data");

            // Add filters (WHERE clause)
            if (!filters.isEmpty()) {
                sql.append(" WHERE ");
                for (int i = 0; i < filters.size(); i++) {
                    sql.append(filters.get(i));
                    if (i < filters.size() - 1) {
                        sql.append(" AND ");
                    }
                }
            }

            // Add GROUP BY clause
            sql.append(" GROUP BY ").append(groupByColumn);

            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery(sql.toString())) {
                // Parse the results into JSON-friendly format
                while (rs.next()) {
                    HashMap<String, Object> row = new HashMap<>();
                    row.put(groupByColumn, rs.getObject(groupByColumn));
                    for (String aggregation : aggregations) {
                        row.put(aggregation, rs.getObject(aggregation));
                    }
                    results.add(row);
                }
            }
        }

        return results;
    }
}