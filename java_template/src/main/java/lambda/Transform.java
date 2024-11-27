import com.opencsv.CSVReader;
import com.opencsv.CSVWriter;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.*;

public class Transform implements RequestHandler<HashMap<String, Object>, HashMap<String, Object>> {

    private static final DateTimeFormatter DATE_FORMAT = DateTimeFormatter.ofPattern("MM/dd/yyyy");
    /**
     * Lambda Function Handler
     * 
     * @param request Hashmap containing request JSON attributes.
     * @param context 
     * @return HashMap that Lambda will automatically convert into JSON.
     */
    public HashMap<String, Object> handleRequest(HashMap<String, Object> request, Context context) {
        // Collect initial data with the inspector
        Inspector inspector = new Inspector();
        inspector.inspectAll();

        // START FUNCTION IMPLEMENTATION
        try {
            String csvData = (String) request.get("csvData");
            String s3Bucket = (String) request.get("s3Bucket");
            String s3Key = (String) request.get("s3Key");

            InputStream csvInputStream;
            if (csvData != null) {
                csvInputStream = new ByteArrayInputStream(csvData.getBytes());
            } else if (s3Bucket != null && s3Key != null) {
                csvInputStream = fetchFromS3(s3Bucket, s3Key);
            } else {
                throw new IllegalArgumentException("No valid CSV input provided.");
            }

            // Process CSV data
            List<String[]> processedData = processCSV(csvInputStream);

            // Write processed data to CSV format
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            writeCSV(processedData, outputStream);
            String processedCSV = outputStream.toString();

            // Add processed CSV to response
            inspector.addAttribute("processedCSV", processedCSV);

        } catch (Exception e) {
            inspector.addAttribute("error", e.getMessage());
        }

        // END FUNCTION IMPLEMENTATION

        // Collect final information such as total runtime and CPU deltas
        inspector.inspectAllDeltas();
        return inspector.finish();
    }
    public List<String[]> processCSV(InputStream csvInputStream) throws IOException {
        try (CSVReader reader = new CSVReader(new InputStreamReader(csvInputStream))) {
            List<String[]> records = reader.readAll();
            if (records.isEmpty()) throw new IllegalArgumentException("CSV is empty!");

            // Extract header and rows
            String[] header = records.get(0);
            List<String[]> rows = records.subList(1, records.size());

            // Add new columns to the header
            String[] updatedHeader = Arrays.copyOf(header, header.length + 2);
            updatedHeader[header.length] = "Order Processing Time";
            updatedHeader[header.length + 1] = "Gross Margin";

            // Process rows
            Map<String, Boolean> processedOrderIds = new HashMap<>();
            List<String[]> processedRows = new ArrayList<>();
            processedRows.add(updatedHeader);

            for (String[] row : rows) {
                // Skip duplicate Order IDs
                String orderId = row[5]; // Assuming "Order ID" is the 6th column (index 5)
                if (processedOrderIds.containsKey(orderId)) continue;
                processedOrderIds.put(orderId, true);

                // Add Order Processing Time
                int processingTime = calculateProcessingTime(row[6], row[7]);
                row = Arrays.copyOf(row, row.length + 2);
                row[row.length - 2] = String.valueOf(processingTime);

                // Transform Order Priority
                row[4] = transformOrderPriority(row[4]);

                // Calculate Gross Margin
                double totalProfit = Double.parseDouble(row[10]) - Double.parseDouble(row[11]); // Total Revenue - Total Cost
                double grossMargin = totalProfit / Double.parseDouble(row[10]);
                row[row.length - 1] = String.format("%.2f", grossMargin);

                processedRows.add(row);
            }

            return processedRows;
        }
    }

    private int calculateProcessingTime(String orderDate, String shipDate) {
        LocalDate order = LocalDate.parse(orderDate, DATE_FORMAT);
        LocalDate ship = LocalDate.parse(shipDate, DATE_FORMAT);
        return (int) java.time.temporal.ChronoUnit.DAYS.between(order, ship);
    }

    private String transformOrderPriority(String priority) {
        return switch (priority) {
            case "L" -> "Low";
            case "M" -> "Medium";
            case "H" -> "High";
            case "C" -> "Critical";
            default -> "Unknown";
        };
    }

    public void writeCSV(List<String[]> data, OutputStream outputStream) throws IOException {
        try (CSVWriter writer = new CSVWriter(new OutputStreamWriter(outputStream))) {
            writer.writeAll(data);
        }
    }

    public InputStream fetchFromS3(String bucketName, String key) {
        // Placeholder for actual S3 fetch logic
        // You can use AWS SDK to retrieve the file:
        // S3Client s3 = S3Client.create();
        // return s3.getObject(GetObjectRequest.builder().bucket(bucketName).key(key).build());
        return null; // Replace with actual logic
    }
}