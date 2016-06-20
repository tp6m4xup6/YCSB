/**
 * Copyright (c) 2010 Yahoo! Inc., Copyright (c) 2016 YCSB contributors. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */
package com.yahoo.ycsb.workloads;

import java.util.Properties;

import com.yahoo.ycsb.*;
import com.yahoo.ycsb.generator.AcknowledgedCounterGenerator;
import com.yahoo.ycsb.generator.ConstantIntegerGenerator;
import com.yahoo.ycsb.generator.CounterGenerator;
import com.yahoo.ycsb.generator.DiscreteGenerator;
import com.yahoo.ycsb.generator.ExponentialGenerator;
import com.yahoo.ycsb.generator.HistogramGenerator;
import com.yahoo.ycsb.generator.HotspotIntegerGenerator;
import com.yahoo.ycsb.generator.NumberGenerator;
import com.yahoo.ycsb.generator.ScrambledZipfianGenerator;
import com.yahoo.ycsb.generator.SequentialGenerator;
import com.yahoo.ycsb.generator.SkewedLatestGenerator;
import com.yahoo.ycsb.generator.UniformIntegerGenerator;
import com.yahoo.ycsb.generator.ZipfianGenerator;
import com.yahoo.ycsb.measurements.Measurements;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Vector;

/**
 * The core benchmark scenario. Represents a set of clients doing simple CRUD
 * operations. The relative proportion of different kinds of operations, and
 * other properties of the workload, are controlled by parameters specified at
 * runtime.
 *
 * Properties to control the client:
 * <UL>
 * <LI><b>fieldcount</b>: the number of fields in a record (default: 10)
 * <LI><b>fieldlength</b>: the size of each field (default: 100)
 * <LI><b>readallfields</b>: should reads read all fields (true) or just one
 * (false) (default: true)
 * <LI><b>writeallfields</b>: should updates and read/modify/writes update all
 * fields (true) or just one (false) (default: false)
 * <LI><b>readproportion</b>: what proportion of operations should be reads
 * (default: 0.95)
 * <LI><b>updateproportion</b>: what proportion of operations should be updates
 * (default: 0.05)
 * <LI><b>insertproportion</b>: what proportion of operations should be inserts
 * (default: 0)
 * <LI><b>scanproportion</b>: what proportion of operations should be scans
 * (default: 0)
 * <LI><b>readmodifywriteproportion</b>: what proportion of operations should be
 * read a record, modify it, write it back (default: 0)
 * <LI><b>requestdistribution</b>: what distribution should be used to select
 * the records to operate on - uniform, zipfian, hotspot, sequential,
 * exponential or latest (default: uniform)
 * <LI><b>maxscanlength</b>: for scans, what is the maximum number of records to
 * scan (default: 1000)
 * <LI><b>scanlengthdistribution</b>: for scans, what distribution should be
 * used to choose the number of records to scan, for each scan, between 1 and
 * maxscanlength (default: uniform)
 * <LI><b>insertstart</b>: for parallel loads and runs, defines the starting
 * record for this YCSB instance (default: 0)
 * <LI><b>insertcount</b>: for parallel loads and runs, defines the number of
 * records for this YCSB instance (default: recordcount)
 * <LI><b>zeropadding</b>: for generating a record sequence compatible with
 * string sort order by 0 padding the record number. Controls the number of 0s
 * to use for padding. (default: 1) For example for row 5, with zeropadding=1
 * you get 'user5' key and with zeropading=8 you get 'user00000005' key. In
 * order to see its impact, zeropadding needs to be bigger than number of digits
 * in the record number.
 * <LI><b>insertorder</b>: should records be inserted in order by key
 * ("ordered"), or in hashed order ("hashed") (default: hashed)
 * </ul>
 */
public class CoreWorkload extends Workload {

    /**
     * The properties which chen10 added,
     */
    //有比例的 family分布
    public static final String FAMILY_PROPORTION = "familyproportion";
    public static final String DEFAULT_FAMILY_PROPORTION = "(family,1)";

    public static final String GROW_FIELD = "growfield";
    public static final String DEFAULT_GROW_FIELD = "false";

    //field 增長的方式   fieldExpension 
    public static final String FIELD_EXPENSION = "fieldExpension";
    public static final String DEFAULT_FIELD_EXPENSION = "uniform";

    //YCSB 讀寫操作的family的數量(default : 1)。
    public static final String FAMILY_COUNT = "familycount";
    public static final String DEFAULT_FAMILY_COUNT = "1";

    //field 插入的欄位數量
    public static final String RECORD_HANDLE_FIELD_COUNT = "recordhandlefield";
    public static final String DEFAULT_HANDLE_RECORD_FIELD_COUNT = "10";

    NumberGenerator growFamilyFieldchooser;

    int familycount;
    int recordhandlefieldcount;

    boolean growfield;
    String defaultFamily = "family";
    String defaultField = "field";

    /**
     * The name of the database table to run queries against.
     */
    public static final String TABLENAME_PROPERTY = "table";

    /**
     * The default name of the database table to run queries against.
     */
    public static final String TABLENAME_PROPERTY_DEFAULT = "usertable";

    public static String table;

    /**
     * The name of the property for the number of fields in a record.
     */
    public static final String FIELD_COUNT_PROPERTY = "fieldcount";

    /**
     * Default number of fields in a record.
     */
    public static final String FIELD_COUNT_PROPERTY_DEFAULT = "10";

    int fieldcount;

    private List<String> fieldnames;

    /**
     * The name of the property for the field length distribution. Options are
     * "uniform", "zipfian" (favouring short records), "constant", and
     * "histogram".
     *
     * If "uniform", "zipfian" or "constant", the maximum field length will be
     * that specified by the fieldlength property. If "histogram", then the
     * histogram will be read from the filename specified in the
     * "fieldlengthhistogram" property.
     */
    public static final String FIELD_LENGTH_DISTRIBUTION_PROPERTY = "fieldlengthdistribution";

    /**
     * The default field length distribution.
     */
    public static final String FIELD_LENGTH_DISTRIBUTION_PROPERTY_DEFAULT = "constant";

    /**
     * The name of the property for the length of a field in bytes.
     */
    public static final String FIELD_LENGTH_PROPERTY = "fieldlength";

    /**
     * The default maximum length of a field in bytes.
     */
    public static final String FIELD_LENGTH_PROPERTY_DEFAULT = "100";

    /**
     * The name of a property that specifies the filename containing the field
     * length histogram (only used if fieldlengthdistribution is "histogram").
     */
    public static final String FIELD_LENGTH_HISTOGRAM_FILE_PROPERTY = "fieldlengthhistogram";

    /**
     * The default filename containing a field length histogram.
     */
    public static final String FIELD_LENGTH_HISTOGRAM_FILE_PROPERTY_DEFAULT = "hist.txt";

    /**
     * Generator object that produces field lengths. The value of this depends
     * on the properties that start with "FIELD_LENGTH_".
     */
    NumberGenerator fieldlengthgenerator;

    /**
     * The name of the property for deciding whether to read one field (false)
     * or all fields (true) of a record.
     */
    public static final String READ_ALL_FIELDS_PROPERTY = "readallfields";

    /**
     * The default value for the readallfields property.
     */
    public static final String READ_ALL_FIELDS_PROPERTY_DEFAULT = "true";

    boolean readallfields;

    /**
     * The name of the property for deciding whether to write one field (false)
     * or all fields (true) of a record.
     */
    public static final String WRITE_ALL_FIELDS_PROPERTY = "writeallfields";

    /**
     * The default value for the writeallfields property.
     */
    public static final String WRITE_ALL_FIELDS_PROPERTY_DEFAULT = "false";

    boolean writeallfields;

    /**
     * The name of the property for deciding whether to check all returned data
     * against the formation template to ensure data integrity.
     */
    public static final String DATA_INTEGRITY_PROPERTY = "dataintegrity";

    /**
     * The default value for the dataintegrity property.
     */
    public static final String DATA_INTEGRITY_PROPERTY_DEFAULT = "false";

    /**
     * Set to true if want to check correctness of reads. Must also be set to
     * true during loading phase to function.
     */
    private boolean dataintegrity;

    /**
     * The name of the property for the proportion of transactions that are
     * reads.
     */
    public static final String READ_PROPORTION_PROPERTY = "readproportion";

    /**
     * The default proportion of transactions that are reads.
     */
    public static final String READ_PROPORTION_PROPERTY_DEFAULT = "0.95";

    /**
     * The name of the property for the proportion of transactions that are
     * updates.
     */
    public static final String UPDATE_PROPORTION_PROPERTY = "updateproportion";

    /**
     * The default proportion of transactions that are updates.
     */
    public static final String UPDATE_PROPORTION_PROPERTY_DEFAULT = "0.05";

    /**
     * The name of the property for the proportion of transactions that are
     * inserts.
     */
    public static final String INSERT_PROPORTION_PROPERTY = "insertproportion";

    /**
     * The default proportion of transactions that are inserts.
     */
    public static final String INSERT_PROPORTION_PROPERTY_DEFAULT = "0.0";

    /**
     * The name of the property for the proportion of transactions that are
     * scans.
     */
    public static final String SCAN_PROPORTION_PROPERTY = "scanproportion";

    /**
     * The default proportion of transactions that are scans.
     */
    public static final String SCAN_PROPORTION_PROPERTY_DEFAULT = "0.0";

    /**
     * The name of the property for the proportion of transactions that are
     * read-modify-write.
     */
    public static final String READMODIFYWRITE_PROPORTION_PROPERTY = "readmodifywriteproportion";

    /**
     * The default proportion of transactions that are scans.
     */
    public static final String READMODIFYWRITE_PROPORTION_PROPERTY_DEFAULT = "0.0";

    /**
     * The name of the property for the the distribution of requests across the
     * keyspace. Options are "uniform", "zipfian" and "latest"
     */
    public static final String REQUEST_DISTRIBUTION_PROPERTY = "requestdistribution";

    /**
     * The default distribution of requests across the keyspace.
     */
    public static final String REQUEST_DISTRIBUTION_PROPERTY_DEFAULT = "uniform";

    /**
     * The name of the property for adding zero padding to record numbers in
     * order to match string sort order. Controls the number of 0s to left pad
     * with.
     */
    public static final String ZERO_PADDING_PROPERTY = "zeropadding";

    /**
     * The default zero padding value. Matches integer sort order
     */
    public static final String ZERO_PADDING_PROPERTY_DEFAULT = "1";

    /**
     * The name of the property for the max scan length (number of records).
     */
    public static final String MAX_SCAN_LENGTH_PROPERTY = "maxscanlength";

    /**
     * The default max scan length.
     */
    public static final String MAX_SCAN_LENGTH_PROPERTY_DEFAULT = "1000";

    /**
     * The name of the property for the scan length distribution. Options are
     * "uniform" and "zipfian" (favoring short scans)
     */
    public static final String SCAN_LENGTH_DISTRIBUTION_PROPERTY = "scanlengthdistribution";

    /**
     * The default max scan length.
     */
    public static final String SCAN_LENGTH_DISTRIBUTION_PROPERTY_DEFAULT = "uniform";

    /**
     * The name of the property for the order to insert records. Options are
     * "ordered" or "hashed"
     */
    public static final String INSERT_ORDER_PROPERTY = "insertorder";

    /**
     * Default insert order.
     */
    public static final String INSERT_ORDER_PROPERTY_DEFAULT = "hashed";

    /**
     * Percentage data items that constitute the hot set.
     */
    public static final String HOTSPOT_DATA_FRACTION = "hotspotdatafraction";

    /**
     * Default value of the size of the hot set.
     */
    public static final String HOTSPOT_DATA_FRACTION_DEFAULT = "0.2";

    /**
     * Percentage operations that access the hot set.
     */
    public static final String HOTSPOT_OPN_FRACTION = "hotspotopnfraction";

    /**
     * Default value of the percentage operations accessing the hot set.
     */
    public static final String HOTSPOT_OPN_FRACTION_DEFAULT = "0.8";

    /**
     * How many times to retry when insertion of a single item to a DB fails.
     */
    public static final String INSERTION_RETRY_LIMIT = "core_workload_insertion_retry_limit";
    public static final String INSERTION_RETRY_LIMIT_DEFAULT = "0";

    /**
     * On average, how long to wait between the retries, in seconds.
     */
    public static final String INSERTION_RETRY_INTERVAL = "core_workload_insertion_retry_interval";
    public static final String INSERTION_RETRY_INTERVAL_DEFAULT = "3";

    NumberGenerator keysequence;

    DiscreteGenerator operationchooser;

    NumberGenerator keychooser;

    NumberGenerator fieldchooser;

    AcknowledgedCounterGenerator transactioninsertkeysequence;

    NumberGenerator scanlength;

    boolean orderedinserts;

    int recordcount;
    int zeropadding;

    int insertionRetryLimit;
    int insertionRetryInterval;

    private Measurements _measurements = Measurements.getMeasurements();

    protected static NumberGenerator getFieldLengthGenerator(Properties p) throws WorkloadException {
        NumberGenerator fieldlengthgenerator;
        String fieldlengthdistribution = p.getProperty(
                FIELD_LENGTH_DISTRIBUTION_PROPERTY, FIELD_LENGTH_DISTRIBUTION_PROPERTY_DEFAULT);
        int fieldlength
                = Integer.parseInt(p.getProperty(FIELD_LENGTH_PROPERTY, FIELD_LENGTH_PROPERTY_DEFAULT));
        String fieldlengthhistogram = p.getProperty(
                FIELD_LENGTH_HISTOGRAM_FILE_PROPERTY, FIELD_LENGTH_HISTOGRAM_FILE_PROPERTY_DEFAULT);
        if (fieldlengthdistribution.compareTo("constant") == 0) {
            fieldlengthgenerator = new ConstantIntegerGenerator(fieldlength);
        } else if (fieldlengthdistribution.compareTo("uniform") == 0) {
            fieldlengthgenerator = new UniformIntegerGenerator(1, fieldlength);
        } else if (fieldlengthdistribution.compareTo("zipfian") == 0) {
            fieldlengthgenerator = new ZipfianGenerator(1, fieldlength);
        } else if (fieldlengthdistribution.compareTo("histogram") == 0) {
            try {
                fieldlengthgenerator = new HistogramGenerator(fieldlengthhistogram);
            } catch (IOException e) {
                throw new WorkloadException(
                        "Couldn't read field length histogram file: " + fieldlengthhistogram, e);
            }
        } else {
            throw new WorkloadException(
                    "Unknown field length distribution \"" + fieldlengthdistribution + "\"");
        }
        return fieldlengthgenerator;
    }

    /**
     * Initialize the scenario. Called once, in the main client thread, before
     * any operations are started.
     */
    @Override
    public void init(Properties p) throws WorkloadException {

        /**
         * chen10 add some properties in init
         */
//        testZipFian(1, 10);
        table = p.getProperty(TABLENAME_PROPERTY, TABLENAME_PROPERTY_DEFAULT);
        System.out.println("CoreWorkload.init: " + table);

        growfield = Boolean.parseBoolean(p.getProperty(GROW_FIELD, DEFAULT_GROW_FIELD));

        fieldcount = Integer.parseInt(p.getProperty(FIELD_COUNT_PROPERTY, FIELD_COUNT_PROPERTY_DEFAULT));
        fieldnames = new ArrayList<String>();
        for (int i = 0; i < fieldcount; i++) {
            fieldnames.add("field" + i);
        }
        fieldlengthgenerator = CoreWorkload.getFieldLengthGenerator(p);

        // YCSB 讀寫操作的family的數量
        familycount = Integer.parseInt(p.getProperty(FAMILY_COUNT, DEFAULT_FAMILY_COUNT));

        //設置每一筆資料要插入幾個物件
        recordhandlefieldcount = Integer.parseInt(p.getProperty(RECORD_HANDLE_FIELD_COUNT, DEFAULT_HANDLE_RECORD_FIELD_COUNT));
        if (recordhandlefieldcount == 0) {
            recordhandlefieldcount = Integer.parseInt(DEFAULT_HANDLE_RECORD_FIELD_COUNT);
        }

        recordcount
                = Integer.parseInt(p.getProperty(Client.RECORD_COUNT_PROPERTY, Client.DEFAULT_RECORD_COUNT));
        if (recordcount == 0) {
            recordcount = Integer.MAX_VALUE;
        }
        String requestdistrib
                = p.getProperty(REQUEST_DISTRIBUTION_PROPERTY, REQUEST_DISTRIBUTION_PROPERTY_DEFAULT);
        int maxscanlength
                = Integer.parseInt(p.getProperty(MAX_SCAN_LENGTH_PROPERTY, MAX_SCAN_LENGTH_PROPERTY_DEFAULT));
        String scanlengthdistrib
                = p.getProperty(SCAN_LENGTH_DISTRIBUTION_PROPERTY, SCAN_LENGTH_DISTRIBUTION_PROPERTY_DEFAULT);

        int insertstart
                = Integer.parseInt(p.getProperty(INSERT_START_PROPERTY, INSERT_START_PROPERTY_DEFAULT));
        int insertcount
                = Integer.parseInt(p.getProperty(INSERT_COUNT_PROPERTY, String.valueOf(recordcount - insertstart)));
        // Confirm valid values for insertstart and insertcount in relation to recordcount
        if (recordcount < (insertstart + insertcount)) {
            System.err.println("Invalid combination of insertstart, insertcount and recordcount.");
            System.err.println("recordcount must be bigger than insertstart + insertcount.");
            System.exit(-1);
        }
        zeropadding
                = Integer.parseInt(p.getProperty(ZERO_PADDING_PROPERTY, ZERO_PADDING_PROPERTY_DEFAULT));

        readallfields = Boolean.parseBoolean(
                p.getProperty(READ_ALL_FIELDS_PROPERTY, READ_ALL_FIELDS_PROPERTY_DEFAULT));
        writeallfields = Boolean.parseBoolean(
                p.getProperty(WRITE_ALL_FIELDS_PROPERTY, WRITE_ALL_FIELDS_PROPERTY_DEFAULT));

        dataintegrity = Boolean.parseBoolean(
                p.getProperty(DATA_INTEGRITY_PROPERTY, DATA_INTEGRITY_PROPERTY_DEFAULT));
        // Confirm that fieldlengthgenerator returns a constant if data
        // integrity check requested.
        if (dataintegrity && !(p.getProperty(
                FIELD_LENGTH_DISTRIBUTION_PROPERTY,
                FIELD_LENGTH_DISTRIBUTION_PROPERTY_DEFAULT)).equals("constant")) {
            System.err.println("Must have constant field size to check data integrity.");
            System.exit(-1);
        }

        if (p.getProperty(INSERT_ORDER_PROPERTY, INSERT_ORDER_PROPERTY_DEFAULT).compareTo("hashed") == 0) {
            orderedinserts = false;
        } else if (requestdistrib.compareTo("exponential") == 0) {
            double percentile = Double.parseDouble(p.getProperty(
                    ExponentialGenerator.EXPONENTIAL_PERCENTILE_PROPERTY,
                    ExponentialGenerator.EXPONENTIAL_PERCENTILE_DEFAULT));
            double frac = Double.parseDouble(p.getProperty(
                    ExponentialGenerator.EXPONENTIAL_FRAC_PROPERTY,
                    ExponentialGenerator.EXPONENTIAL_FRAC_DEFAULT));
            keychooser = new ExponentialGenerator(percentile, recordcount * frac);
        } else {
            orderedinserts = true;
        }

        keysequence = new CounterGenerator(insertstart);
        operationchooser = createOperationGenerator(p);

        growFamilyFieldchooser = createFieldGrower(p);

        transactioninsertkeysequence = new AcknowledgedCounterGenerator(recordcount);
        if (requestdistrib.compareTo("uniform") == 0) {
            keychooser = new UniformIntegerGenerator(insertstart, insertstart + insertcount - 1);
        } else if (requestdistrib.compareTo("sequential") == 0) {
            keychooser = new SequentialGenerator(insertstart, insertstart + insertcount - 1);
        } else if (requestdistrib.compareTo("zipfian") == 0) {
            // it does this by generating a random "next key" in part by taking the modulus over the
            // number of keys.
            // If the number of keys changes, this would shift the modulus, and we don't want that to
            // change which keys are popular so we'll actually construct the scrambled zipfian generator
            // with a keyspace that is larger than exists at the beginning of the test. that is, we'll predict
            // the number of inserts, and tell the scrambled zipfian generator the number of existing keys
            // plus the number of predicted keys as the total keyspace. then, if the generator picks a key
            // that hasn't been inserted yet, will just ignore it and pick another key. this way, the size of
            // the keyspace doesn't change from the perspective of the scrambled zipfian generator
            final double insertproportion = Double.parseDouble(
                    p.getProperty(INSERT_PROPORTION_PROPERTY, INSERT_PROPORTION_PROPERTY_DEFAULT));
            int opcount = Integer.parseInt(p.getProperty(Client.OPERATION_COUNT_PROPERTY));
            int expectednewkeys = (int) ((opcount) * insertproportion * 2.0); // 2 is fudge factor

            keychooser = new ScrambledZipfianGenerator(insertstart, insertstart + insertcount + expectednewkeys);
        } else if (requestdistrib.compareTo("latest") == 0) {
            keychooser = new SkewedLatestGenerator(transactioninsertkeysequence);
        } else if (requestdistrib.equals("hotspot")) {
            double hotsetfraction
                    = Double.parseDouble(p.getProperty(HOTSPOT_DATA_FRACTION, HOTSPOT_DATA_FRACTION_DEFAULT));
            double hotopnfraction
                    = Double.parseDouble(p.getProperty(HOTSPOT_OPN_FRACTION, HOTSPOT_OPN_FRACTION_DEFAULT));
            keychooser = new HotspotIntegerGenerator(insertstart, insertstart + insertcount - 1,
                    hotsetfraction, hotopnfraction);
        } else {
            throw new WorkloadException("Unknown request distribution \"" + requestdistrib + "\"");
        }

        fieldchooser = new UniformIntegerGenerator(0, fieldcount - 1);
        
       

        if (scanlengthdistrib.compareTo("uniform") == 0) {
            scanlength = new UniformIntegerGenerator(1, maxscanlength);
        } else if (scanlengthdistrib.compareTo("zipfian") == 0) {
            scanlength = new ZipfianGenerator(1, maxscanlength);
        } else {
            throw new WorkloadException(
                    "Distribution \"" + scanlengthdistrib + "\" not allowed for scan length");
        }

        insertionRetryLimit = Integer.parseInt(p.getProperty(
                INSERTION_RETRY_LIMIT, INSERTION_RETRY_LIMIT_DEFAULT));
        insertionRetryInterval = Integer.parseInt(p.getProperty(
                INSERTION_RETRY_INTERVAL, INSERTION_RETRY_INTERVAL_DEFAULT));

        
//        testGrowFieldValues();
        testGetValueCostTime();
    }

//    public void testGrowFieldValues() {
//
//        for (int count = 0; count < 10; count++) {
//
//            int fieldchoosedcount =0
//            boolean[] fieldchoosed = new boolean[fieldcount];
//            
//            StringBuilder sb = new StringBuilder();
//    while(fieldchoosedcount<3){
//            for (int i = 0; i < 3; i++) {
//                int nextFieldValue = fieldchooser.nextValue().intValue();
//                if (fieldchoosed[nextFieldValue]) {
//                    continue;
//                } else {
//                    fieldchoosed[nextFieldValue] = true;
//                }
//
//                String fieldkey = fieldnames.get(nextFieldValue);
//
//                sb.append(fieldkey + ",");
//
//            }
//
//            System.out.println(sb.toString());
//        }
//    }
//
//    public void testZipFian(int start, int finish) {
//
//        NumberGenerator testZipfianFgenerator = new ZipfianGenerator(start, finish);
//
//        System.out.println("Test  zipfian");
//        System.out.println("\tnextString\tlastString\tnextValue\tlastvalue");
//
//        StringBuilder newRecord = new StringBuilder();
//
//        HashMap<Number, Integer> count = new HashMap<Number, Integer>();
//        for (int i = start; i <= finish; i++) {
//            System.out.println(testZipfianFgenerator.lastValue());
//
//            Number value = testZipfianFgenerator.nextValue();
//            if (count.containsKey(value)) {
//                count.put(value, count.get(value) + 1);
//            } else {
//                count.put(value, 1);
//                newRecord.append("(" + i + "," + value + ")");
//            }
//
////                System.out.println(i+",\t\t"+testZipfianFgenerator.nextValue()+"\t\t"+testZipfianFgenerator.lastValue());
////                System.out.println(i+"\t\t"+testZipfianFgenerator.nextString()+",\t\t"+testZipfianFgenerator.lastString());
////                System.out.println(i+"\t\t"+testZipfianFgenerator.nextString()+",\t\t"+testZipfianFgenerator.lastString()+"\t\t"+testZipfianFgenerator.nextValue()+"\t\t"+testZipfianFgenerator.lastValue());
//        }
//
////        transactioninsertkeysequence = new AcknowledgedCounterGenerator(finish);
////
////        int keynum;
////        for (int i = 0; i < finish - start; i++) {
////            do {
////                keynum = testZipfianFgenerator.nextValue().intValue();
////            } while (keynum > transactioninsertkeysequence.lastValue());
////            System.out.println("keychooser:"+keynum);
////        }
////        
//        for (Entry<Number, Integer> entry : count.entrySet()) {
//            System.out.println(entry.getKey() + "," + entry.getValue());
//        }
//        System.out.println("size:" + count.size());
//        System.out.println(newRecord.toString());
//
//    }
//
    
    
    void testGetValueCostTime(){
        
        
        
    }
    
    
    public String buildKeyName(long keynum) {
        if (!orderedinserts) {
            keynum = Utils.hash(keynum);
        }
        String value = Long.toString(keynum);
        int fill = zeropadding - value.length();
        String prekey = "user";
        for (int i = 0; i < fill; i++) {
            prekey += '0';
        }
        return prekey + value;
    }

    /**
     * Builds a value for a randomly chosen field.
     */
    private HashMap<String, ByteIterator> buildSingleValue(String key) {
        HashMap<String, ByteIterator> value = new HashMap<String, ByteIterator>();

        String fieldkey = fieldnames.get(fieldchooser.nextValue().intValue());
        ByteIterator data;
        if (dataintegrity) {
            data = new StringByteIterator(buildDeterministicValue(key, fieldkey));
        } else {
            // fill with random data
            data = new RandomByteIterator(fieldlengthgenerator.nextValue().longValue());
        }
        value.put(fieldkey, data);

        return value;
    }

    /**
     * auther : chen10 Builds values for growing fields by chen10
     */
    public HashMap<String, HashMap<String, ByteIterator>> buildGrowingFamilyFieldValues(String key) {

        HashMap<String, HashMap<String, ByteIterator>> familyFieldValues = new HashMap<String, HashMap<String, ByteIterator>>();

        int totalFildCount = fieldcount * familycount;
        boolean[] fieldchoosed = buildGrowingFieldsNumber();

        String family = defaultFamily;
        String field = defaultField;
        HashMap<String, ByteIterator> value = new HashMap<String, ByteIterator>();

        for (int i = 0; i < totalFildCount; i++) {
            if (i == 0) {
                family = defaultFamily + "0";
            }

            if (fieldchoosed[i]) {
                field = defaultField + (i % fieldcount);

                ByteIterator data;
                if (dataintegrity) {
                    data = new StringByteIterator(buildDeterministicValue(key, field));
                } else {
                    // fill with random data
                    data = new RandomByteIterator(fieldlengthgenerator.nextValue().longValue());
                }
                value.put(field, data);
            }

            if (i % fieldcount == fieldcount - 1 && i != 0) {
                if (value.size() > 0) {
                    familyFieldValues.put(family, value);
                }
                family = defaultFamily + ((i + 1) / fieldcount);
                value = new HashMap<String, ByteIterator>();
            }

        }

        return familyFieldValues;
    }

    /**
     * auther : chen10 Builds values for normal fields by chen10
     */
    public HashMap<String, HashMap<String, ByteIterator>> buildFullFamilyFieldValues(String key) {
        HashMap<String, HashMap<String, ByteIterator>> familyFieldValues = new HashMap<String, HashMap<String, ByteIterator>>();

        for (int i = 0; i < familycount; i++) {
            HashMap<String, ByteIterator> value = new HashMap<String, ByteIterator>();
            String family = defaultFamily + i;

//        for (int i = 0; i < recordfieldcount; i++) {
            for (int j = 0; j < fieldcount; j++) {
                String fieldkey = defaultField + j;
                ByteIterator data;
                if (dataintegrity) {
                    data = new StringByteIterator(buildDeterministicValue(key, fieldkey));
                } else {
                    // fill with random data
                    data = new RandomByteIterator(fieldlengthgenerator.nextValue().longValue());
                }

                value.put(fieldkey, data);
            }

            familyFieldValues.put(family, value);
        }
        return familyFieldValues;
    }

    /**
     * Build new familyFieldValuesMap for new interface
     */
    private HashMap<String, HashMap<String, ByteIterator>> buildFamilyFieldValueMap(String keynum) {

        HashMap<String, HashMap<String, ByteIterator>> familyFieldValuesMap = new HashMap<String, HashMap<String, ByteIterator>>();
        if (growfield) {
            System.out.println("這個");
            familyFieldValuesMap = buildGrowingFamilyFieldValues(keynum);
            System.out.println("這個節數");
            
        } else {
            System.out.println("那個");
            familyFieldValuesMap = buildFullFamilyFieldValues(keynum);
        }
        return familyFieldValuesMap;
    }

    /**
     * Build new familyFieldMaps for new interface
     */
    private HashMap<String, Set<String>> buildFamilyFieldMaps() {

        HashMap<String, Set<String>> familyFieldMaps = new HashMap<String, Set<String>>();
        if (growfield) {
            System.out.println("buildFamilyFieldMaps 這個");
            familyFieldMaps = buildGrowingFamilyFieldMaps();
        } else {
            System.out.println("buildFamilyFieldMaps 那個");
            familyFieldMaps = buildFullFamilyFieldMaps();
        }
        return familyFieldMaps;
    }

    HashMap<String, Set<String>> buildFullFamilyFieldMaps() {
        HashMap<String, Set<String>> familyFieldMaps = new HashMap<String, Set<String>>();

        Set<String> fields = new HashSet<String>();
        for (int i = 0; i < fieldcount; i++) {
            String fieldkey = defaultField + i;
            ByteIterator data;
            fields.add(fieldkey);
        }

        for (int i = 0; i < familycount; i++) {
            String family = defaultFamily + i;
            familyFieldMaps.put(family, fields);
        }
        return familyFieldMaps;
    }

    HashMap<String, Set<String>> buildGrowingFamilyFieldMaps() {

        HashMap<String, Set<String>> familyFieldMaps = new HashMap<String, Set<String>>();
        
        boolean[] fieldchoosed = buildGrowingFieldsNumber();

        int totalFildCount = fieldcount * familycount;
        String family = defaultFamily;
        String field = defaultField;
        Set<String> fields = new HashSet();
        
        for (int i = 0; i < totalFildCount; i++) {
            if (i == 0) {
                family = defaultFamily + "0";
            }

            if (fieldchoosed[i]) {
                field = defaultField + (i % fieldcount);
                System.out.println("<"+family+","+field+">");
                fields.add(field);
            }

            if (i % fieldcount == fieldcount - 1 && i != 0) {
                if (fields.size() > 0) {
                    familyFieldMaps.put(family, fields);
                }
                family = defaultFamily + ((i + 1) / fieldcount);
                fields =  new HashSet();
            }
        }
        

        return familyFieldMaps;

    }
    
    boolean[] buildGrowingFieldsNumber(){
        int totalFildCount = fieldcount * familycount;
        boolean[] fieldchoosed = new boolean[totalFildCount];

        int temp_recordfieldcount = recordhandlefieldcount;
        //先選數字
        while (temp_recordfieldcount > 0) {
            int nextFieldValue = growFamilyFieldchooser.nextValue().intValue();
            
            
            if (fieldchoosed[nextFieldValue]) {
                continue;
            } else {
                fieldchoosed[nextFieldValue] = true;
                temp_recordfieldcount--;
            }
        } 
        return fieldchoosed;
    }

    /**
     * Builds values for all fields.
     */
    private HashMap<String, ByteIterator> buildValues(String key) {
        HashMap<String, ByteIterator> values = new HashMap<String, ByteIterator>();

        for (String fieldkey : fieldnames) {
            ByteIterator data;
            if (dataintegrity) {
                data = new StringByteIterator(buildDeterministicValue(key, fieldkey));
            } else {
                // fill with random data
                data = new RandomByteIterator(fieldlengthgenerator.nextValue().longValue());
            }
            values.put(fieldkey, data);
        }
        return values;
    }

    /**
     * Build a deterministic value given the key information.
     */
    private String buildDeterministicValue(String key, String fieldkey) {
        int size = fieldlengthgenerator.nextValue().intValue();
        StringBuilder sb = new StringBuilder(size);
        sb.append(key);
        sb.append(':');
        sb.append(fieldkey);
        while (sb.length() < size) {
            sb.append(':');
            sb.append(sb.toString().hashCode());
        }
        sb.setLength(size);

        return sb.toString();
    }

    /**
     * Do one insert operation. Because it will be called concurrently from
     * multiple client threads, this function must be thread safe. However,
     * avoid synchronized, or the threads will block waiting for each other, and
     * it will be difficult to reach the target throughput. Ideally, this
     * function would have no side effects other than DB operations.
     */
    @Override
    public boolean doInsert(DB db, Object threadstate) {

        Long startTime = System.currentTimeMillis();
        int keynum = keysequence.nextValue().intValue();
        String dbkey = buildKeyName(keynum);

        HashMap<String, HashMap<String, ByteIterator>> familyFieldValuesMap = buildFamilyFieldValueMap(dbkey);

        
        Long afterCreateValueTime = System.currentTimeMillis();
//        HashMap<String, ByteIterator> values;
//        if (growfield) {
//            values = buildGrowingValues(dbkey);
//        } else {
//        values = buildValues(dbkey);
//        }
        Status status;
        int numOfRetries = 0;
        do {
//      status = db.insert(table, dbkey, values);     
            status = db.insert(table, dbkey, familyFieldValuesMap);

            if (status == Status.OK) {
                break;
            }
            // Retry if configured. Without retrying, the load process will fail
            // even if one single insertion fails. User can optionally configure
            // an insertion retry limit (default is 0) to enable retry.
            if (++numOfRetries <= insertionRetryLimit) {
                System.err.println("Retrying insertion, retry count: " + numOfRetries);
                try {
                    // Sleep for a random number between [0.8, 1.2)*insertionRetryInterval.
                    int sleepTime = (int) (1000 * insertionRetryInterval * (0.8 + 0.4 * Math.random()));
                    Thread.sleep(sleepTime);
                } catch (InterruptedException e) {
                    break;
                }

            } else {
                System.err.println("Error inserting, not retrying any more. number of attempts: " + numOfRetries
                        + "Insertion Retry Limit: " + insertionRetryLimit);
                break;

            }
        } while (true);

         Long afterScanTime = System.currentTimeMillis();
         
        System.out.println("create time : "+ (afterCreateValueTime - startTime));
        System.out.println("insert time : "+ (afterScanTime -afterCreateValueTime));
        return (status == Status.OK);
    }

    /**
     * Do one transaction operation. Because it will be called concurrently from
     * multiple client threads, this function must be thread safe. However,
     * avoid synchronized, or the threads will block waiting for each other, and
     * it will be difficult to reach the target throughput. Ideally, this
     * function would have no side effects other than DB operations.
     */
    @Override
    public boolean doTransaction(DB db, Object threadstate) {
        switch (operationchooser.nextString()) {
            case "READ":
                doTransactionRead(db);
                break;
            case "UPDATE":
                doTransactionUpdate(db);
                break;
            case "INSERT":
                doTransactionInsert(db);
                break;
            case "SCAN":
                doTransactionScan(db);
                break;
            default:
                doTransactionReadModifyWrite(db);
        }

        return true;
    }

    /**
     * Results are reported in the first three buckets of the histogram under
     * the label "VERIFY". Bucket 0 means the expected data was returned. Bucket
     * 1 means incorrect data was returned. Bucket 2 means null data was
     * returned when some data was expected.
     */
    protected void verifyRow(String key, HashMap<String, ByteIterator> cells) {
        Status verifyStatus = Status.OK;
        long startTime = System.nanoTime();
        if (!cells.isEmpty()) {
            for (Map.Entry<String, ByteIterator> entry : cells.entrySet()) {
                if (!entry.getValue().toString().equals(buildDeterministicValue(key, entry.getKey()))) {
                    verifyStatus = Status.UNEXPECTED_STATE;
                    break;
                }
            }
        } else {
            // This assumes that null data is never valid
            verifyStatus = Status.ERROR;
        }
        long endTime = System.nanoTime();
        _measurements.measure("VERIFY", (int) (endTime - startTime) / 1000);
        _measurements.reportStatus("VERIFY", verifyStatus);
    }

    int nextKeynum() {
        int keynum;
        if (keychooser instanceof ExponentialGenerator) {
            do {
                keynum = transactioninsertkeysequence.lastValue() - keychooser.nextValue().intValue();
            } while (keynum < 0);
        } else {
            do {
                keynum = keychooser.nextValue().intValue();
            } while (keynum > transactioninsertkeysequence.lastValue());
        }
        return keynum;
    }

    public void doTransactionRead(DB db) {
        // choose a random key
        int keynum = nextKeynum();

        String keyname = buildKeyName(keynum);

        HashMap<String, Set<String>> familyFieldMaps = buildFamilyFieldMaps();
        HashMap<String, ByteIterator> cells = new HashMap<String, ByteIterator>();

        db.read(table, keyname, familyFieldMaps, cells);
        System.out.println("doTransactionRead " + table + " " + keyname);

        for (Map.Entry<String, ByteIterator> result : cells.entrySet()) {
            System.out.print(result.getKey() + ",");
//            
//            System.out.println(result.getValue() + ",");
        }
        System.out.println();

//        if (!readallfields) {
//            // read a random field
//            String fieldname = fieldnames.get(fieldchooser.nextValue().intValue());
//
//            fields = new HashSet<String>();
//            fields.add(fieldname);
//        } else if (dataintegrity) {
//            // pass the full field list if dataintegrity is on for verification
//            fields = new HashSet<String>(fieldnames);
//        }
//        HashMap<String, ByteIterator> cells = new HashMap<String, ByteIterator>();
//    db.read(table, keyname, fields, cells);
//
//        System.out.println("read " + table + " " + " " + keyname);
//        db.read(table, keyname, familyFieldMaps, cells);
//
//        for (Map.Entry<String, ByteIterator> entry : cells.entrySet()) {
//            System.out.println(cells);
//        }
//
//        if (dataintegrity) {
//            verifyRow(keyname, cells);
//        }
    }

    public void doTransactionReadModifyWrite(DB db) {
        // choose a random key
        int keynum = nextKeynum();
        String keyname = buildKeyName(keynum);

        HashMap<String, Set<String>> familyFieldMaps = buildFamilyFieldMaps();
        HashMap<String, ByteIterator> cells = new HashMap<String, ByteIterator>();
        HashMap<String, HashMap<String, ByteIterator>> familyFieldValuesMap = buildFamilyFieldValueMap(keyname);

//        HashSet<String> fields = null;
//        if (!readallfields) {
//            // read a random field
//            String fieldname = fieldnames.get(fieldchooser.nextValue().intValue());
//
//            fields = new HashSet<String>();
//            fields.add(fieldname);
//        }
//        HashMap<String, ByteIterator> values;
//        if (writeallfields) {
//            // new data for all the fields
//            values = buildValues(keyname);
//        } else {
//            // update a random field
//            values = buildSingleValue(keyname);
//        }
        // do the transaction
//        HashMap<String, ByteIterator> cells = new HashMap<String, ByteIterator>();
        long ist = _measurements.getIntendedtartTimeNs();
        long st = System.nanoTime();
//    db.read(table, keyname, fields, cells);
//    db.update(table, keyname, values);

        db.read(table, keyname, familyFieldMaps, cells);
        db.update(table, keyname, familyFieldValuesMap);

        System.out.println("doTransactionReadModifyWrite " + "," + keyname + ":");

        for (Map.Entry<String, ByteIterator> result : cells.entrySet()) {
            System.out.print(result.getKey() + ",");
        }
        System.out.println();

        long en = System.nanoTime();

//        if (dataintegrity) {
//            verifyRow(keyname, cells);
//        }
        _measurements.measure("READ-MODIFY-WRITE", (int) ((en - st) / 1000));
        _measurements.measureIntended("READ-MODIFY-WRITE", (int) ((en - ist) / 1000));
    }

    public void doTransactionScan(DB db) {
        // choose a random key
        int keynum = nextKeynum();

        String startkeyname = buildKeyName(keynum);

        // choose a random scan length
        int len = scanlength.nextValue().intValue();

        HashMap<String, Set<String>> familyFieldMaps = buildFamilyFieldMaps();
        Vector<HashMap<String, ByteIterator>> rows = new Vector<HashMap<String, ByteIterator>>();

        System.out.println("scan " + table + " " + startkeyname + ",len = " + len);
        db.scan(table, startkeyname, len, familyFieldMaps, rows);

        for (HashMap<String, ByteIterator> row : rows) {
            for (Map.Entry<String, ByteIterator> result : row.entrySet()) {
                System.out.print(result.getKey() + ",");
            }
            System.out.println();
        }

//        HashSet<String> fields = null;
//
//        if (!readallfields) {
//            // read a random field
//            String fieldname = fieldnames.get(fieldchooser.nextValue().intValue());
//
//            fields = new HashSet<String>();
//            fields.add(fieldname);
//        }
//
//    db.scan(table, startkeyname, len, fields, new Vector<HashMap<String, ByteIterator>>());
//        System.out.println("scan " + table + " " + defaultFamily + "," + len + " " + startkeyname);
//        Vector<HashMap<String, ByteIterator>> result = new Vector<HashMap<String, ByteIterator>>();
//        db.scan(table, startkeyname, defaultFamily, len, fields, result);
        if (rows.size() > 0) {
            System.out.println("get " + rows.size() + ",");
        } else {
            System.out.println("get " + rows.size());
        }

    }

    public void doTransactionUpdate(DB db) {
        // choose a random key
        int keynum = nextKeynum();

        String keyname = buildKeyName(keynum);

//        HashMap<String, ByteIterator> values;
//
//        if (writeallfields) {
//            // new data for all the fields
//            values = buildValues(keyname);
//        } else {
//            // update a random field
//            values = buildSingleValue(keyname);
//        }
//    db.update(table, keyname, values);
//TODO
        HashMap<String, HashMap<String, ByteIterator>> familyFieldValuesMap = buildFamilyFieldValueMap(keyname);
        System.out.println("update " + table + " " + defaultFamily + " " + keyname);
        db.update(table, keyname, familyFieldValuesMap);

    }

    public void doTransactionInsert(DB db) {
        // choose the next key
        int keynum = transactioninsertkeysequence.nextValue();

        try {
            String dbkey = buildKeyName(keynum);
            HashMap<String, HashMap<String, ByteIterator>> familyFieldValuesMap = buildFamilyFieldValueMap(dbkey);
            
            
//            HashMap<String, ByteIterator> values = buildValues(dbkey);
//      db.insert(table, dbkey, values);
//TODO
            db.insert(table, dbkey, familyFieldValuesMap);

        } finally {
            transactioninsertkeysequence.acknowledge(keynum);
        }
    }

    /**
     * Creates a weighted discrete values with database operations for a
     * workload to perform. Weights/proportions are read from the properties
     * list and defaults are used when values are not configured. Current
     * operations are "READ", "UPDATE", "INSERT", "SCAN" and "READMODIFYWRITE".
     *
     * @param p The properties list to pull weights from.
     * @return A generator that can be used to determine the next operation to
     * perform.
     * @throws IllegalArgumentException if the properties object was null.
     */
    public static DiscreteGenerator createOperationGenerator(final Properties p) {
        if (p == null) {
            throw new IllegalArgumentException("Properties object cannot be null");
        }
        final double readproportion = Double.parseDouble(
                p.getProperty(READ_PROPORTION_PROPERTY, READ_PROPORTION_PROPERTY_DEFAULT));
        final double updateproportion = Double.parseDouble(
                p.getProperty(UPDATE_PROPORTION_PROPERTY, UPDATE_PROPORTION_PROPERTY_DEFAULT));
        final double insertproportion = Double.parseDouble(
                p.getProperty(INSERT_PROPORTION_PROPERTY, INSERT_PROPORTION_PROPERTY_DEFAULT));
        final double scanproportion = Double.parseDouble(
                p.getProperty(SCAN_PROPORTION_PROPERTY, SCAN_PROPORTION_PROPERTY_DEFAULT));
        final double readmodifywriteproportion = Double.parseDouble(p.getProperty(
                READMODIFYWRITE_PROPORTION_PROPERTY, READMODIFYWRITE_PROPORTION_PROPERTY_DEFAULT));

        final DiscreteGenerator operationchooser = new DiscreteGenerator();
        if (readproportion > 0) {
            operationchooser.addValue(readproportion, "READ");
        }

        if (updateproportion > 0) {
            operationchooser.addValue(updateproportion, "UPDATE");
        }

        if (insertproportion > 0) {
            operationchooser.addValue(insertproportion, "INSERT");
        }

        if (scanproportion > 0) {
            operationchooser.addValue(scanproportion, "SCAN");
        }

        if (readmodifywriteproportion > 0) {
            operationchooser.addValue(readmodifywriteproportion, "READMODIFYWRITE");
        }
        return operationchooser;
    }

    public NumberGenerator createFieldGrower(final Properties p) throws WorkloadException {
        NumberGenerator fieldGrower;
        String fieldExpension
                = p.getProperty(FIELD_EXPENSION, DEFAULT_FIELD_EXPENSION);
System.out.println("fieldExpension:"+fieldExpension);
        if (fieldExpension.compareTo("uniform") == 0) {
            fieldGrower = new UniformIntegerGenerator(0,( fieldcount * familycount) - 1);
        } else if(fieldExpension.compareTo("hotspot")  == 0 ){
            double hotsetfraction= Double.parseDouble(p.getProperty(HOTSPOT_DATA_FRACTION, HOTSPOT_DATA_FRACTION_DEFAULT));
            double hotopnfraction= Double.parseDouble(p.getProperty(HOTSPOT_OPN_FRACTION, HOTSPOT_OPN_FRACTION_DEFAULT));
            fieldGrower = new  HotspotIntegerGenerator(0,( fieldcount * familycount) - 1,
                    hotsetfraction, hotopnfraction);
        }
        else {
            throw new WorkloadException("Unknown request distribution \"" + fieldExpension + "\"");
        }

        return fieldGrower;
    }

    public DiscreteGenerator createFamilyGenerator(final Properties p) {
        if (p == null) {
            throw new IllegalArgumentException("Properties object cannot be null");
        }

        final DiscreteGenerator familychooser = new DiscreteGenerator();

        String familydistribution = p.getProperty(FAMILY_PROPORTION, DEFAULT_FAMILY_PROPORTION);
        String[] familyproportionps = familydistribution.split(";");

        for (String family : familyproportionps) {
            if (family.startsWith("(") && family.endsWith(")")) {
                System.out.println(family);
                family = family.substring(1, family.length() - 1);
                String[] onefamilyproportion = family.split(",");

                if (onefamilyproportion.length != 2) {
                    throw new IllegalArgumentException("porperty \'familyproportion\' has wrong format:" + familydistribution);
                }

                Double proportion = Double.parseDouble(onefamilyproportion[1]);
                if (proportion > 0) {
                    familychooser.addValue(proportion, onefamilyproportion[0]);
                }
            }
        }
        return familychooser;
    }

}
