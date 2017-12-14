package tw.com.mitake.hbase.io.request;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class Main {
    private static String URL;
    private static boolean SORT_BY_WRITE;
    private static int DIRECTION;
    private static Document DOC;

    public static void main(String[] args) {
        initial(args);

        List<RegionRequest> regionRequests = gatherData();

        if (regionRequests.isEmpty()) {
            System.out.println("No data");

            System.exit(0);
        }

        sortData(regionRequests);

        printData(regionRequests);
    }

    private static void initial(String[] args) {
        if (args.length != 3) {
            System.out.println("Please input HBase Region Server URL (e.g. http://10.1.18.168:60030), sort field (e.g. 'w' or 'r'), sort direction (e.g. 'inc' or 'desc')");

            System.exit(1);
        }

        URL = args[0];

        if (args[1].equalsIgnoreCase("w")) {
            SORT_BY_WRITE = true;
        } else if (args[1].equalsIgnoreCase("r")) {
            SORT_BY_WRITE = false;
        } else {
            System.out.println("Please input 'w' or 'r'");

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
    }

    private static List<RegionRequest> gatherData() {
        System.out.println("Gathering data...\n");

        List<RegionRequest> regionRequests = new ArrayList<RegionRequest>();

        try {
            DOC = Jsoup.connect(URL + "/rs-status?filter=all").get();

            Elements regions = DOC.select("#tab_regionRequestStats > table > tbody > tr:not(:first-child)");

            for (Element region : regions) {
                Element retionNameElem = region.selectFirst("td:nth-child(1)");
                Element readCountElem = region.selectFirst("td:nth-child(2)");
                Element writeCountElem = region.selectFirst("td:nth-child(3)");

                String[] regionNameParts = retionNameElem.text().split(",");

                String regionName = regionNameParts[0] + "," + regionNameParts[1];

                System.out.println("Name: " + regionName);

                RegionRequest regionRequest = new RegionRequest(regionName, Long.valueOf(readCountElem.text()), Long.valueOf(writeCountElem.text()));

                regionRequests.add(regionRequest);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return regionRequests;
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

        System.out.println("\nRegion Servers\n");
        System.out.println("Request Per Second\tRead Request Count\tWrite Request Count");
        System.out.println("------------------\t------------------\t-------------------");
        System.out.println(requestPerSecond + "\t" + readRequestCount + "\t" + writeRequestCount);
    }

    public static class RegionRequest implements Comparable<RegionRequest> {
        private String name;
        private long readCount;
        private long writeCount;

        public RegionRequest(String name, long readCount, long writeCount) {
            this.name = name;
            this.readCount = readCount;
            this.writeCount = writeCount;
        }

        public int compareTo(RegionRequest o) {
            if (SORT_BY_WRITE) {
                return DIRECTION * (int) (this.writeCount - o.writeCount);
            } else {
                return DIRECTION * (int) (this.readCount - o.readCount);
            }
        }
    }
}