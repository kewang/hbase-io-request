package tw.kewang;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.io.IOException;

public class App {
    private static String URL;

    public static void main(String[] args) {
        if (args.length != 1) {
            System.out.println("Please input HBase URL");

            System.exit(1);
        }

        URL = args[0];

        try {
            Document doc = Jsoup.connect(URL + "/master-status").get();

            Elements tables = doc.select("#tab_userTables > table > tbody > tr > td:nth-child(2)");

            for (Element table : tables) {
                Document tableDoc = Jsoup.connect(URL + "/table.jsp?name=" + table.text()).get();

                Element request = tableDoc.selectFirst("body > div.container-fluid.content > div:nth-child(2) > table:nth-child(4) > tbody > tr:nth-child(2) > td:nth-child(6)");

                System.out.println(table.text() + " request: " + request.text());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}