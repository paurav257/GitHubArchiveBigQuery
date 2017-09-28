package gitarchive;

import com.google.cloud.bigquery.*;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

/**
 * Created by paurav on 9/28/17.
 *
 * This class executes the Google BigQuery and process the data to compute the adjusted mean and median for the commit
 * messages seen so far.
 */
public class DataAccessClass {
    final static Logger logger = Logger.getLogger(DataAccessClass.class);
    private String startDate;
    private String endDate;
    private String startTime;
    private String endTime;

    public DataAccessClass(String startDate, String endDate, String startTime, String endTime) {
        this.startDate = startDate;
        this.endDate = endDate;
        this.startTime = startTime;
        this.endTime = endTime;
    }

    /**
     * Process the response from the sum query to get the sum of length of commit messages.
     * TODO: We can not run this query and aggregate the sum from second query.
     * @param bigQuery
     * @param query
     * @return
     */
    public static long executeSumQuery(BigQuery bigQuery, String query) {
        long sum = 0;

        QueryResponse response = queryConfigAndResponse(bigQuery, query);
        QueryResult result = response.getResult();
        while (result != null) {
            for (List<FieldValue> row : result.iterateAll()) {
                if (!row.get(0).isNull()) {
                    sum = row.get(0).getLongValue();
                    logger.info("Sum: " + row.get(0).getLongValue() + " ||| Count: " + row.get(1).getLongValue());
                }
            }
            result = result.getNextPage();
        }
        return sum;
    }

    /**
     * Process the response from the query to get all the length of commit messages.
     * @param bigQuery
     * @param query
     */
    public static void executeLengthQuery(BigQuery bigQuery, String query) {

        // Get the results.
        QueryResponse response = queryConfigAndResponse(bigQuery, query);
        QueryResult result = response.getResult();
        while (result != null) {
            for (List<FieldValue> row : result.iterateAll()) {
                if (!row.get(0).isNull()) {
                    App.addToReservior(row.get(0).getLongValue());
                    App.lastSeen = row.get(1).getTimestampValue() / 1000;
                }
            }
            result = result.getNextPage();
        }
    }

    /**
     * Helper method which configures and executes the given query
     * @param bigQuery
     * @param query
     * @return the response from the query
     */
    public static QueryResponse queryConfigAndResponse(BigQuery bigQuery,
                                                       String query) {
        QueryJobConfiguration queryJobConfiguration =
                QueryJobConfiguration.newBuilder(query)
                        // Standard SQL syntax is required for parameterized queries.
                        // See: https://cloud.google.com/bigquery/sql-reference/
                        .setUseLegacySql(true)
                        .setUserDefinedFunctions(getUDFList())
                        .build();

        // Create a job ID so that we can safely retry.
        JobId jobId = JobId.of(UUID.randomUUID().toString());
        Job queryJob = bigQuery.create(JobInfo.newBuilder(queryJobConfiguration).setJobId(jobId).build());

        // Wait for the query to complete.
        try {
            queryJob = queryJob.waitFor();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (TimeoutException e) {
            e.printStackTrace();
        }

        // Check for errors
        if (queryJob == null) {
            throw new RuntimeException("Job no longer exists");
        } else if (queryJob.getStatus().getError() != null) {
            // You can also look at queryJob.getStatus().getExecutionErrors() for all
            // errors, not just the latest one.
            throw new RuntimeException(queryJob.getStatus().getError().toString());
        }

        // Get the results.
        QueryResponse response = bigQuery.getQueryResults(jobId);

        return response;
    }

    /**
     * This a User Defined Function in JavaScript, used by Google BigQuery to process multiple commit messages under
     * same push event.
     * @return
     */
    private static List<UserDefinedFunction> getUDFList() {
        List<UserDefinedFunction> udfList = new
                ArrayList<UserDefinedFunction>();
        udfList.add(UserDefinedFunction.inline("function findSumCount(r, emit) {\n" +
                "  var obj = JSON.parse(r.payload)\n" +
                "  for(var field in obj) {\n" +
                "    if(obj[field].message.length > 0){\n" +
                "      emit({len: obj[field].message.length,\n" +
                "            cnt: 1,\n" +
                "           created_at: r.created_at});\n" +
                "    }\n" +
                "  }\n" +
                "}\n" +
                "  \n" +
                "// UDF registration\n" +
                "bigquery.defineFunction(\n" +
                "  'findSumCount',  // Name used to call the function from SQL\n" +
                "\n" +
                "  ['payload','created_at'],  // Input column names\n" +
                "\n" +
                "  // JSON representation of the output schema\n" +
                "  [{name: 'len', type: 'integer'},\n" +
                "   {name: 'cnt', type: 'integer'},\n" +
                "   {name: 'created_at', type: 'timestamp'}\n" +
                "  ],\n" +
                "\n" +
                "  findSumCount  // The function reference\n" +
                ");"));

        return udfList;
    }
}
