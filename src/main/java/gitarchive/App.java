
package gitarchive;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import static java.lang.Math.ceil;

/**
 * Created by paurav on 9/28/17.
 *
 * This program should read Github’s commit messages from Google BigQuery and then calculate and print mean and
 * median length of commit messages from the data. There’s one tweak though. The data on BigQuery gets updated every
 * hour and we want to adjust our mean and median values periodically as well to reflect new data.
 *
 * This program pulls data from Google BigQuery at every timeWindow.
 *
 * Adjusted mean is calculated using the old mean and the sum and count from the new data observations, and
 * maintaining a count for the total data observations seen until now.
 *
 * Adjusted Median is calculated from a Random sample of the data. As we don't have a data structure which helps us
 * to store the data observation in primary memory at the same time. Also, it is not feasible to store the data on disk.
 *
 * For more info I would suggest to read the task description pdf in this repo.
 *
 */
public class App extends TimerTask {
    private static final String SUM_COUNT =
            "SELECT sum(len), SUM(cnt) FROM findSumCount( SELECT JSON_EXTRACT(payload, '$.commits') AS payload, " +
                    "created_at FROM githubarchive.day.@startDate, githubarchive.day.@endDate " +
                    "WHERE type = 'PushEvent' and payload is not null " +
                    "AND TIMESTAMP(created_at) > TIMESTAMP('@startTime')" +
                    "AND TIMESTAMP(created_at) < TIMESTAMP('@endTime'));";
    private static final String MEDIAN =
            "SELECT len, created_at FROM findSumCount( SELECT JSON_EXTRACT(payload, '$.commits') AS payload, created_at " +
                    "FROM githubarchive.day.@startDate, githubarchive.day.@endDate " +
                    "WHERE type = 'PushEvent' and payload is not null " +
                    "AND TIMESTAMP(created_at) > TIMESTAMP('@startTime')" +
                    "AND TIMESTAMP(created_at) < TIMESTAMP('@endTime')) order by created_at;;";

    // Time window in milliseconds
    static final int timeWindow = 1000 * 60 * 60 * 6;

    // Size of the sampling used for medain calculation
    static final int SIZE = 1000000;

    // Initial seed for the Random functions used
    static final long SEED = 167357;

    final static Logger logger = Logger.getLogger(App.class);

    // Arraylist to store reservoir samples
    static List<Long> reservoir = new ArrayList<Long>();

    // Using BigInterger and BigDecimal beacuse they handle overflow
    static BigInteger count = BigInteger.valueOf(0);
    static BigInteger oldCount = BigInteger.valueOf(0);
    static Random random = new Random();
    static BigDecimal mean = BigDecimal.ZERO;

    // Time to track all the records seen so far. This is required because GitHub might push the data when we are no
    // longer in that time window
    static long lastSeen = 0l;

    /**
     * Run for the timer task, ran after every timeWindow interval of time.
     */
    public void run() {
        logger.info("Starting BigQuery Application ...");

        BigQuery bigQuery = BigQueryOptions.getDefaultInstance().getService();
        calcMeanMedian(bigQuery);
    }

    public static void main(String[] args) {
        PropertyConfigurator.configure("log4j.properties");

        TimerTask timerTask = new App();
        //running timer task as daemon thread
        Timer timer = new Timer();
        timer.scheduleAtFixedRate(timerTask, 0, timeWindow);
    }

    /**
     * This method do the calculation of mean and median, after the new data is fetched from the Google BigQuery.
     *
     * @param bigQuery Google BigQuery connection instance
     */
    private void calcMeanMedian(BigQuery bigQuery) {

        long sum = 0;

        logger.debug("getstartDate() -- " + getstartDate());
        logger.debug("getEndDate() -- " + getEndDate());
        logger.debug("getStartTime() -- " + getStartTime());
        logger.debug("getEndTime() -- " + getEndTime());

        String startDate = getstartDate();
        String endDate = getEndDate();
        String startTime = getStartTime();
        String endTime = getEndTime();

        String query = SUM_COUNT.replace("@startDate", startDate).replace("@endDate", endDate)
                .replace("@startTime", startTime).replace("@endTime", endTime);
        logger.debug("SumQuery -- " + query);
        DataAccessClass dataAccessClass = new DataAccessClass(startDate, endDate, startTime, endTime);
        sum = dataAccessClass.executeSumQuery(bigQuery, query);

        query = MEDIAN.replace("@startDate", startDate).replace("@endDate", endDate)
                .replace("@startTime", startTime).replace("@endTime", endTime);
        logger.debug(query);
        oldCount = count;
        dataAccessClass.executeLengthQuery(bigQuery, query);

        long median = getMedian();
        mean = getMean(sum);
        logger.debug("lastSeen -- " + lastSeen);
        logger.debug("Count -- " + count);
        logger.debug("oldCount -- " + oldCount);
        logger.info("Median -- " + median);
        logger.info("Mean -- " + mean);

    }

    /**
     * This method calculates and returns the mean, using all the operations from BigDecimal.
     * @param sum long representing sum of the data observations from the new data discovered.
     * @return BigDecimal representing the new adjusted mean.
     */
    private static BigDecimal getMean(long sum) {
        if (count.compareTo(BigInteger.ZERO) == 0)
            return mean;
        return mean.multiply(new BigDecimal(oldCount)).add(BigDecimal.valueOf(sum)).divide(new BigDecimal(count), 3, 1);
    }

    /**
     * This method calculates and returns the median, by sorting sample and then finding the Median.
     * @return long representing the new adjusted median.
     */
    private static long getMedian() {
        Collections.sort(reservoir);
        if (count.compareTo(BigInteger.ZERO) == 0)
            return 0l;
        int size = (reservoir.size() < SIZE) ? reservoir.size() : SIZE;
        if (size % 2 != 0)
            return reservoir.get((int) ceil(size / 2));
        else {
            logger.debug(reservoir.get((size / 2)));
            logger.debug(size / 2);
            logger.debug(reservoir.get((size / 2) + 1));
            return ((reservoir.get(size / 2) + reservoir.get((size / 2) + 1)) / 2);
        }
    }

    /**
     * This method adds the num to the sample using the Reservior sampling technique.
     * @param num the data obervation which is needed to add to the sample.
     */
    static void addToReservior(long num) {
        if (BigInteger.valueOf(SIZE).compareTo(count) == 1) {
            reservoir.add(num);
            count = count.add(BigInteger.ONE);
        } else {
            BigInteger p = new BigInteger(count.bitLength(), new Random(SEED));
            count = count.add(BigInteger.ONE);
            if (BigInteger.valueOf(SIZE).compareTo(p) == 1) {
                reservoir.set(p.intValue() - 1, num);
            }
        }
    }

    /**
     * This method returns the start time formated for the Google BigQuery.
     * @return
     */
    private static String getStartTime() {
        Date date = new Date(System.currentTimeMillis() - timeWindow);
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss zzz");
        simpleDateFormat.setTimeZone(new SimpleTimeZone(SimpleTimeZone.UTC_TIME, "UTC"));
        if (lastSeen != 0)
            date = new Date(lastSeen);
        else {
            try {
                lastSeen = simpleDateFormat.parse(simpleDateFormat.format(date)).getTime();
            } catch (ParseException e) {
                e.printStackTrace();
            }
        }
        return simpleDateFormat.format(date);
    }

    /**
     * This method returns the end date formated for the Google BigQuery table names.
     * @return
     */
    private static String getEndDate() {
        Date date = new Date(System.currentTimeMillis());
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMdd");
        simpleDateFormat.setTimeZone(new SimpleTimeZone(SimpleTimeZone.UTC_TIME, "UTC"));
        return simpleDateFormat.format(date);
    }

    /**
     * This method returns the start date formated for the Google BigQuery table names.
     * @return
     */
    private static String getstartDate() {
        Date date = new Date(System.currentTimeMillis() - timeWindow);
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMdd");
        simpleDateFormat.setTimeZone(new SimpleTimeZone(SimpleTimeZone.UTC_TIME, "UTC"));
        if (lastSeen != 0)
            date = new Date(lastSeen);
        return simpleDateFormat.format(date);
    }

    /**
     * This method returns the end time formated for the Google BigQuery.
     * @return
     */
    private static String getEndTime() {
        Date date = new Date(System.currentTimeMillis());
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss zzz");
        simpleDateFormat.setTimeZone(new SimpleTimeZone(SimpleTimeZone.UTC_TIME, "UTC"));
        return simpleDateFormat.format(date);
    }
}
