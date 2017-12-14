package tw.com.mitake.hbase.io.request;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class Main {
    private static String URL;
    private static int DIRECTION;
    private static Document DOC;

    public static void main(String[] args) {
        initial(args);

        List<TableRequest> tableRequests = gatherData();

        sortData(tableRequests);

        printData(tableRequests);
    }

    private static void initial(String[] args) {
        if (args.length != 2) {
            System.out.println("Please input HBase URL (e.g. http://10.1.18.168:60010) and sort direction (e.g. 'inc' or 'desc')");

            System.exit(1);
        }

        URL = args[0];

        if (args[1].equalsIgnoreCase("inc")) {
            DIRECTION = 1;
        } else if (args[1].equalsIgnoreCase("desc")) {
            DIRECTION = -1;
        } else {
            System.out.println("Please input 'inc' or 'desc'");

            System.exit(1);
        }
    }

    private static List<TableRequest> gatherData() {
        System.out.println("Gathering data...\n");

        List<TableRequest> tableRequests = new ArrayList<TableRequest>();

        try {
            DOC = Jsoup.connect(URL + "/master-status").get();

            Elements tables = DOC.select("#tab_userTables > table > tbody > tr > td:nth-child(2)");

            for (Element table : tables) {
                Document docTable = Jsoup.connect(URL + "/table.jsp?name=" + table.text()).get();

                Element request = docTable.selectFirst("body > div.container-fluid.content > div:nth-child(2) > table:nth-child(4) > tbody > tr:nth-child(2) > td:nth-child(6)");

                System.out.println("Name: " + table.text());

                TableRequest tableRequest = new TableRequest(table.text(), Long.valueOf(request.text()));

                tableRequests.add(tableRequest);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return tableRequests;
    }

    private static void sortData(List<TableRequest> tableRequests) {
        System.out.println("\nSorting data...\n");

        Collections.sort(tableRequests, new Comparator<TableRequest>() {
            public int compare(TableRequest t1, TableRequest t2) {
                return DIRECTION * (int) (t1.requests - t2.requests);
            }
        });
    }

    private static void printData(List<TableRequest> tableRequests) {
        for (TableRequest tableRequest : tableRequests) {
            System.out.println(tableRequest.name + "\t" + tableRequest.requests);
        }

        Element regionServer = DOC.selectFirst("#tab_requestStats > table > tbody > tr:nth-child(2)");

        long requestPerSecond = Long.valueOf(regionServer.selectFirst("td:nth-child(2)").text());
        long readRequestCount = Long.valueOf(regionServer.selectFirst("td:nth-child(3)").text());
        long writeRequestCount = Long.valueOf(regionServer.selectFirst("td:nth-child(4)").text());

        System.out.println("\nRegion Servers\n");
        System.out.println("Request Per Second\tRead Request Count\tWrite Request Count");
        System.out.println("------------------\t------------------\t-------------------");
        System.out.println(requestPerSecond + "\t" + readRequestCount + "\t" + writeRequestCount);
    }

    public static class TableRequest {
        private String name;
        private long requests;

        public TableRequest(String name, long requests) {
            this.name = name;
            this.requests = requests;
        }
    }
}