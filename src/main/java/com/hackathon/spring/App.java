package com.hackathon.spring;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.hackathon.transformation.ETL;
import org.apache.commons.io.FilenameUtils;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.Row;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import org.apache.poi.openxml4j.exceptions.InvalidFormatException;
import org.apache.poi.ss.usermodel.*;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static org.apache.spark.sql.functions.col;


@RestController
@RequestMapping("/data")
@SpringBootApplication
public class App implements CommandLineRunner {

    private SparkConf conf;
    private JavaSparkContext sc;
    private SparkSession spark;
    private SQLContext sqlContext;

    Dataset<Row> dfCSV ;
    Dataset<Row> dfXLS ;

    String path1 ;
    String path2 ;

    Dataset<Row> select;

    public static void main(String[] args) {

        SpringApplication.run(App.class, args);
    }

    @Override
    public void run(String... args) throws IOException {



        conf = new SparkConf().setMaster("local").setAppName("App");
        sc = new JavaSparkContext(conf);
        spark = SparkSession
                .builder()
                .appName("App")
                .config("spark.some.config.option", "some-value")
                .getOrCreate();
        sqlContext = new org.apache.spark.sql.SQLContext(spark);

        System.out.println("-------------------------------------------------------------------------" );





        path1 = "src/main/resources/data/file1.csv";
        path2 = "src/main/resources/data/file2.xlsx";

        String request1 = "SELECT * FROM table1";
        String request2 = "SELECT * FROM table2";

        dfCSV = spark.read().format("csv").option("header", "true").load(path1);
        dfCSV.createOrReplaceTempView("table1");

        dfXLS = spark.read()
                .format("com.crealytics.spark.excel")
                .option("sheetName", "EA_result_1351") // Required
                .option("useHeader", "true") // Requiredr
                .load(path2);
        dfXLS.createOrReplaceTempView("table2");

        System.out.println(headerJson(dfCSV,"csv"));
        System.out.println(headerJson(dfXLS,"xls"));

        Dataset<Row> select1 = select(request1);
        Dataset<Row> select2 = select(request2);

        Dataset<Row> combine = combine(dfCSV,dfXLS,colCSV, colXLS);




        System.out.println("+++++++++++++++");

                System.out.println("--------------------------------------------------------------------------------------------" );

    }
    public Dataset grouby (Dataset<Row> df, String[] colGroupBy , String[]MAXs,String[]MINs,String[]FIRSTs,String[]LASTs,String[]AVGs){

        ArrayList<String> al=new ArrayList<String>();
        String group="",aggr="";
        for (String colo : colGroupBy) { group = group + colo + " , ";  }
        for (String max : MAXs) { aggr = "max("+aggr + max + "), ";  }
        for (String min : MINs) { aggr = "min("+aggr + min + "), ";  }
        for (String first : FIRSTs) { aggr = "first("+aggr + first + "), ";  }
        for (String last : LASTs) { aggr = "last("+aggr + last + "), ";  }
        for (String avg : AVGs) { aggr = "avg("+aggr + avg + "), ";  }
        System.out.println(group);
        System.out.println(aggr);
        return df.groupBy(group)
                .agg(aggr.split(","));
    }

    public Dataset<Row> combine (Dataset<Row> dfCSV,Dataset<Row> dfXLS,String[] colCSV, String[] colXLS) {
        return dfCSV.join(dfXLS, col("df1Key").equalTo(col("df2Key")), "inner");
    }
    public Dataset<Row> select( String request){
        return spark.sql(request);

    }
    @RequestMapping(value = "/header/{file}",method = RequestMethod.GET)
    public String getheader(@PathVariable("file") int file) {
        if (file == 1)
            return headerJson(dfCSV,"csv");
        else
            return headerJson(dfXLS,"xls");
    }

    public String headerJson(Dataset<Row> d, String path){
        if (path =="csv") {
            String[] t = d.schema().fieldNames()[0].split(";");
            GsonBuilder gsonBuilder = new GsonBuilder();
            Gson gson = gsonBuilder.create();
            return  gson.toJson(t) ;
        }else{
            String[] t = d.schema().fieldNames();
            GsonBuilder gsonBuilder = new GsonBuilder();
            Gson gson = gsonBuilder.create();
            return  gson.toJson(t) ;
        }
    }

    @RequestMapping(value = "/flowchart",method = RequestMethod.GET)
    public String getQuery(@PathVariable("file") String query) {
        select = select(query + " FROM table1");
        return "Ok";
    }

}
