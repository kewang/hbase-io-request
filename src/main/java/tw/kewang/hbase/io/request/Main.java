package tw.kewang.hbase.io.request;

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

    public static void main(String[] args) {
        if (args.length != 2) {
            System.out.println("Please input HBase URL (e.g. http://10.1.18.168:60010) and sort direction (e.g. 'inc' or 'desc'");

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

        List<TableRequest> tableRequests = new ArrayList<TableRequest>();

        try {
            System.out.println("Gathering data...\n");

            Document doc = Jsoup.connect(URL + "/master-status").get();

            Elements tables = doc.select("#tab_userTables > table > tbody > tr > td:nth-child(2)");

            for (Element table : tables) {
                Document tableDoc = Jsoup.connect(URL + "/table.jsp?name=" + table.text()).get();

                Element request = tableDoc.selectFirst("body > div.container-fluid.content > div:nth-child(2) > table:nth-child(4) > tbody > tr:nth-child(2) > td:nth-child(6)");

                System.out.println("Name: " + table.text());

                TableRequest tableRequest = new TableRequest(table.text(), Long.valueOf(request.text()));

                tableRequests.add(tableRequest);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        System.out.println("\nSorting data...\n");

        Collections.sort(tableRequests, new Comparator<TableRequest>() {
            public int compare(TableRequest t1, TableRequest t2) {
                return DIRECTION * (int) (t1.requests - t2.requests);
            }
        });

        for (TableRequest tableRequest : tableRequests) {
            System.out.println(tableRequest.name + "\t" + tableRequest.requests);
        }
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