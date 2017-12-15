package tw.com.mitake.hbase.io.request;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class Main {
    private static final String[] HEADERS = {"Region Name", "Read Count", "Write Count", "Total Count"};

    private static String URL;
    private static SortField SORT_FIELD;
    private static int DIRECTION;
    private static boolean MERGE;
    private static String OUTPUT_FILENAME;
    private static Document DOC;

    public static void main(String[] args) {
        initial(args);

        List<RegionRequest> regionRequests = gatherData();

        if (regionRequests.isEmpty()) {
            System.out.println("No data");

            System.exit(0);
        }

        if (MERGE) {
            regionRequests = mergeData(regionRequests);
        }

        sortData(regionRequests);

        printData(regionRequests);

        if (OUTPUT_FILENAME != null && OUTPUT_FILENAME.length() != 0) {
            try {
                writeData(regionRequests);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private static void initial(String[] args) {
        if (args.length < 4) {
            System.out.println("Please input HBase Region Server URL (e.g. 10.1.18.168:60030), sort field (e.g. 'w' or 'r' or 't'), sort direction (e.g. 'inc' or 'desc'), merge (e.g. 'y' or 'n'), output filename");

            System.exit(1);
        }

        URL = "http://" + args[0];

        if (args[1].equalsIgnoreCase("w")) {
            SORT_FIELD = SortField.WRITE;
        } else if (args[1].equalsIgnoreCase("r")) {
            SORT_FIELD = SortField.READ;
        } else if (args[1].equalsIgnoreCase("t")) {
            SORT_FIELD = SortField.TOTAL;
        } else {
            System.out.println("Please input 'w' or 'r' or 't'");

            System.exit(1);
        }

        if (args[2].equalsIgnoreCase("inc")) {
            DIRECTION = 1;
        } else if (args[2].equalsIgnoreCase("desc")) {
            DIRECTION = -1;
        } else {
            System.out.println("Please input 'inc' or 'desc'");

            System.exit(1);
        }

        if (args[3].equalsIgnoreCase("y")) {
            MERGE = true;
        } else if (args[3].equalsIgnoreCase("n")) {
            MERGE = false;
        } else {
            System.out.println("Please input 'y' or 'n'");

            System.exit(1);
        }

        if (args.length == 5) {
            OUTPUT_FILENAME = args[4];
        }
    }

    private static List<RegionRequest> gatherData() {
        System.out.println("Gathering data...\n");

        List<RegionRequest> regionRequests = new ArrayList<RegionRequest>();

        try {
            DOC = Jsoup.connect(URL + "/rs-status?filter=all").get();

            Elements regions = DOC.select("#tab_regionRequestStats > table > tbody > tr:not(:first-child)");

            for (Element region : regions) {
                Element regionNameElem = region.selectFirst("td:nth-child(1)");
                Element readCountElem = region.selectFirst("td:nth-child(2)");
                Element writeCountElem = region.selectFirst("td:nth-child(3)");

                String[] regionNameParts = regionNameElem.text().split(",");

                String regionName = regionNameParts[0];

                boolean split = false;

                if (regionNameParts[1].length() != 0) {
                    split = true;

                    if (!MERGE) {
                        regionName += "," + regionNameParts[1];
                    }
                }

                System.out.println("Name: " + regionName);

                RegionRequest regionRequest = new RegionRequest(regionName, split, Long.valueOf(readCountElem.text()), Long.valueOf(writeCountElem.text()));

                regionRequests.add(regionRequest);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return regionRequests;
    }

    private static List<RegionRequest> mergeData(List<RegionRequest> regionRequests) {
        List<RegionRequest> newData = new ArrayList<RegionRequest>();

        for (RegionRequest regionRequest : regionRequests) {
            boolean found = false;

            for (RegionRequest newDataElem : newData) {
                if (newDataElem.name.equals(regionRequest.name) && regionRequest.split) {
                    newDataElem.readCount += regionRequest.readCount;
                    newDataElem.writeCount += regionRequest.writeCount;
                    newDataElem.totalCount += regionRequest.totalCount;

                    found = true;
                }
            }

            if (!found) {
                newData.add(regionRequest);
            }
        }

        return newData;
    }

    private static void sortData(List<RegionRequest> regionRequests) {
        System.out.println("\nSorting data...\n");

        Collections.sort(regionRequests);
    }

    private static void printData(List<RegionRequest> regionRequests) {
        System.out.println("Name\tRead Count\tWrite Count\tTotal Count\n");

        for (RegionRequest regionRequest : regionRequests) {
            System.out.println(regionRequest.name + "\t" + regionRequest.readCount + "\t" + regionRequest.writeCount + "\t" + (regionRequest.readCount + regionRequest.writeCount));
        }

        Element regionServer = DOC.selectFirst("#tab_requestStats > table > tbody > tr:nth-child(2)");

        long requestPerSecond = Long.valueOf(regionServer.selectFirst("td:nth-child(1)").text());
        long readRequestCount = Long.valueOf(regionServer.selectFirst("td:nth-child(2)").text());
        long writeRequestCount = Long.valueOf(regionServer.selectFirst("td:nth-child(3)").text());

        System.out.println("\nRequest Per Second\tRead Request Count\tWrite Request Count");
        System.out.println("------------------\t------------------\t-------------------");
        System.out.println(requestPerSecond + "\t" + readRequestCount + "\t" + writeRequestCount);
    }

    private static void writeData(List<RegionRequest> regionRequests) throws IOException {
        System.out.print("\nWriting data to " + OUTPUT_FILENAME + " ... ");

        FileWriter writer = new FileWriter(OUTPUT_FILENAME);

        CSVPrinter csv = new CSVPrinter(writer, CSVFormat.DEFAULT.withHeader(HEADERS));

        for (RegionRequest regionRequest : regionRequests) {
            csv.printRecord(regionRequest.name, regionRequest.readCount, regionRequest.writeCount, regionRequest.totalCount);
        }

        csv.flush();
        writer.flush();

        System.out.println("OK");
    }

    public static class RegionRequest implements Comparable<RegionRequest> {
        private String name;
        private boolean split;
        private long readCount;
        private long writeCount;
        private long totalCount;

        public RegionRequest(String name, boolean split, long readCount, long writeCount) {
            this.name = name;
            this.split = split;
            this.readCount = readCount;
            this.writeCount = writeCount;
            this.totalCount = readCount + writeCount;
        }

        public int compareTo(RegionRequest o) {
            switch (SORT_FIELD) {
                case WRITE:
                    return DIRECTION * (int) (this.writeCount - o.writeCount);
                case READ:
                    return DIRECTION * (int) (this.readCount - o.readCount);
                case TOTAL:
                default:
                    return DIRECTION * (int) (this.totalCount - o.totalCount);
            }
        }
    }

    public enum SortField {
        WRITE, READ, TOTAL
    }
}