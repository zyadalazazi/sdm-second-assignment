package exercise_4;

import com.clearspring.analytics.util.Lists;
import org.apache.commons.io.IOUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.graphframes.GraphFrame;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class Exercise_4 {
	private static java.util.List<Row> vertices_list;
	private static java.util.List<Row> edges_list;


	public static void main(String[] args) {
		loadFile();
	}

	public static void loadFile() {
		vertices_list = new ArrayList<Row>();
		edges_list = new ArrayList<Row>();

		try(FileInputStream inputStream = new FileInputStream("src/main/resources/wiki-vertices.txt");
			FileInputStream inputStream2 = new FileInputStream("src/main/resources/wiki-edges.txt")) {

			// Read the first file wiki-vertices
			List<String> lines = IOUtils.readLines(inputStream);
			for (String line: lines) {
//				System.out.println(line);
				String[] items = line.split("\t");
				vertices_list.add(RowFactory.create(items[0], items[1]));
			}
			// Read the second file wiki-edges
			lines = IOUtils.readLines(inputStream2);
			for (String line: lines) {
				String[] items = line.split("\t");
				edges_list.add(RowFactory.create(items[0], items[1]));
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public static void wikipedia(JavaSparkContext ctx, SQLContext sqlCtx) {

		loadFile();

		JavaRDD<Row> vertices_rdd = ctx.parallelize(vertices_list);

		StructType vertices_schema = new StructType(new StructField[]{
				new StructField("id", DataTypes.StringType, true, new MetadataBuilder().build()),
				new StructField("name", DataTypes.StringType, true, new MetadataBuilder().build())
		});

		Dataset<Row> vertices =  sqlCtx.createDataFrame(vertices_rdd, vertices_schema);

		JavaRDD<Row> edges_rdd = ctx.parallelize(edges_list);

		StructType edges_schema = new StructType(new StructField[]{
				new StructField("src", DataTypes.StringType, true, new MetadataBuilder().build()),
				new StructField("dst", DataTypes.StringType, true, new MetadataBuilder().build())
		});

		Dataset<Row> edges = sqlCtx.createDataFrame(edges_rdd, edges_schema);

		GraphFrame gf = GraphFrame.apply(vertices,edges);

//		System.out.println(gf);
//		gf.edges().show();
//		gf.vertices().show();

		GraphFrame pageRankResult = gf.pageRank().maxIter(20).resetProbability(0.1).run();
		System.out.println("========== PageRank results =========");
		pageRankResult.vertices()
				.orderBy(org.apache.spark.sql.functions.col("pagerank").desc())
				.show(10);
	}

}
