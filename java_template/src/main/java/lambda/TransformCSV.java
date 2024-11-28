package lambda;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3Object;

import saaf.Inspector;
import saaf.Response;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;

public class TransformCSV implements RequestHandler<Request, HashMap<String, Object>> {

    @Override
    public HashMap<String, Object> handleRequest(Request request, Context context) {
        Inspector inspector = new Inspector();
        inspector.inspectAll();

        String bucketname = request.getBucketname(); // S3 bucket name
        String filename = request.getFilename();    // Output file name in S3
        String csvData = request.getName();         // CSV content directly passed in payload (optional)
        List<List<String>> csvContent;

        try {
            // Step 1: Fetch or Read CSV Data
            if (csvData != null && !csvData.isEmpty()) {
                // Parse directly from the payload
                csvContent = parseCSVFromPayload(csvData);
            } else {
                // Read from S3 if no CSV payload provided
                csvContent = readCSVFromS3(bucketname, filename);
            }

            // Step 2: Perform transformations
            List<List<String>> transformedData = transformData(csvContent);

            // Step 3: Write transformed data to S3
            uploadToS3(bucketname, filename, writeCSVToStream(transformedData));

            // Add success response
            Response response = new Response();
            response.setValue("Transformed CSV uploaded to S3: " + filename);
            inspector.consumeResponse(response);

        } catch (Exception e) {
            e.printStackTrace();
            inspector.addAttribute("error", e.getMessage());
        }

        inspector.inspectAllDeltas();
        return inspector.finish();
    }

    /**
     * Parses CSV from payload data.
     */
    private List<List<String>> parseCSVFromPayload(String csvData) {
        List<List<String>> csvContent = new ArrayList<>();
        String[] lines = csvData.split("\n");
        for (String line : lines) {
            csvContent.add(Arrays.asList(line.split(",")));
        }
        return csvContent;
    }

    /**
     * Reads CSV data from S3 into a List of rows.
     */
    private List<List<String>> readCSVFromS3(String bucketname, String filename) throws IOException {
        AmazonS3 s3Client = AmazonS3ClientBuilder.standard().build();
        S3Object s3Object = s3Client.getObject(bucketname, filename);

        List<List<String>> csvData = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(s3Object.getObjectContent()))) {
            String line;
            while ((line = reader.readLine()) != null) {
                csvData.add(Arrays.asList(line.split(",")));
            }
        }
        return csvData;
    }

    /**
     * Transforms CSV data with the required operations:
     * - Adds [Order Processing Time] column.
     * - Transforms [Order Priority] values.
     * - Adds [Gross Margin] column.
     * - Removes duplicates based on [Order ID].
     */
    private List<List<String>> transformData(List<List<String>> csvData) {
        List<List<String>> transformedData = new ArrayList<>();
        Map<String, Integer> columnIndices = getColumnIndices(csvData.get(0));

        // Add headers for new columns
        List<String> header = new ArrayList<>(csvData.get(0));
        header.add("Order Processing Time");
        header.add("Gross Margin");
        transformedData.add(header);

        // Track unique Order IDs
        Set<String> uniqueOrderIDs = new HashSet<>();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("MM/dd/yyyy");

        // Process each row
        for (int i = 1; i < csvData.size(); i++) {
            List<String> row = new ArrayList<>(csvData.get(i));
            String orderId = row.get(columnIndices.get("Order ID"));

            if (!uniqueOrderIDs.add(orderId)) {
                continue; // Skip duplicates
            }

            // Calculate Order Processing Time
            LocalDate orderDate = LocalDate.parse(row.get(columnIndices.get("Order Date")), formatter);
            LocalDate shipDate = LocalDate.parse(row.get(columnIndices.get("Ship Date")), formatter);
            long processingTime = java.time.temporal.ChronoUnit.DAYS.between(orderDate, shipDate);
            row.add(String.valueOf(processingTime));

            // Transform Order Priority
            String orderPriority = row.get(columnIndices.get("Order Priority"));
            row.set(columnIndices.get("Order Priority"), transformOrderPriority(orderPriority));

            // Calculate Gross Margin
            double totalProfit = Double.parseDouble(row.get(columnIndices.get("Total Profit")));
            double totalRevenue = Double.parseDouble(row.get(columnIndices.get("Total Revenue")));
            double grossMargin = totalProfit / totalRevenue;
            row.add(String.format("%.2f", grossMargin));

            transformedData.add(row);
        }

        return transformedData;
    }

    /**
     * Helper method to map column headers to indices.
     */
    private Map<String, Integer> getColumnIndices(List<String> header) {
        Map<String, Integer> indices = new HashMap<>();
        for (int i = 0; i < header.size(); i++) {
            indices.put(header.get(i), i);
        }
        return indices;
    }

    /**
     * Transforms the Order Priority value.
     */
    private String transformOrderPriority(String priority) {
        switch (priority) {
            case "L": return "Low";
            case "M": return "Medium";
            case "H": return "High";
            case "C": return "Critical";
            default: return priority;
        }
    }

    /**
     * Writes CSV data to an in-memory ByteArrayOutputStream.
     */
    private ByteArrayOutputStream writeCSVToStream(List<List<String>> csvData) throws IOException {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(outputStream, StandardCharsets.UTF_8))) {
            for (List<String> row : csvData) {
                writer.write(String.join(",", row));
                writer.newLine();
            }
        }
        return outputStream;
    }

    /**
     * Uploads the transformed CSV data to S3.
     */
    private void uploadToS3(String bucketname, String keyName, ByteArrayOutputStream outputStream) {
        AmazonS3 s3Client = AmazonS3ClientBuilder.standard().build();
        byte[] bytes = outputStream.toByteArray();

        try (InputStream inputStream = new ByteArrayInputStream(bytes)) {
            s3Client.putObject(bucketname, keyName, inputStream, null);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
