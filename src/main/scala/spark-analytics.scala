import org.apache.spark.sql.functions.{from_unixtime,unix_timestamp,_}

import org.apache.spark.sql.{SparkSession,types}

// This program was created to compute some analytics for a data set that shows movies and users data sets.
// Gets informations about moviews, users, and ratings.


object SparkAnalytics {
  def main(args: Array[String]): Unit = {

    // Define columns for data sets
    val ratings_columns = Seq("user_id","item_id","rating","timestamp")
    val movies_columns = Seq( "movie_id",
      "movie_title",
      "release_date",
      "video_release_date",
      "IMDb_URL",
      "unknown",
      "Action",
      "Adventure",
      "Animation",
      "Children\'s",
      "Comedy",
      "Crime",
      "Documentary",
      "Drama",
      "Fantasy",
      "Film-Noir",
      "Horror",
      "Musical",
      "Mystery",
      "Romance",
      "Sci-Fi",
      "Thriller",
      "War",
      "Western"
    )
    // user data set
    val user_columns = Seq ( "user_id","age" ,"gender","occupation", "zip_code")
    // Movies Genres
    val occupations_columns = Seq ("ocupation_name")
    // Geners for the movies
    val genres_columns = Seq("genre_name")
    // Generes Names
    val genres_names = Seq ("unknown","Action","Adventure","Animation","Children's","Comedy",
      "Crime",
      "Documentary",
      "Drama",
      "Fantasy",
      "Film-Noir",
      "Horror",
      "Musical",
      "Mystery",
      "Romance",
      "Sci-Fi",
      "Thriller",
      "War",
      "Western"
  )

    val sparkSession = SparkSession.builder()
      .master("local")
      .appName("Spark Analytics")
      .getOrCreate()


    val ratings = sparkSession.read.format("csv").option("header","false")
      .option("sep","\t")
      .option("escape","\t")
      .csv("file:////home/ojuarezwork412/MovieLens/u.data")
      .toDF(ratings_columns: _*)
    //ratings.printSchema()
    //ratings.show(false)
    val movies = sparkSession.read.format("csv").option("header", "false")
      .option("sep", "|")
      .option("escape", "\t")
      .csv("file:////home/ojuarezwork412/MovieLens/u.item")
      .toDF(movies_columns: _*)
    //movies.printSchema()
    //movies.show(false)
    val occupations = sparkSession.read.format("csv").option("header", "false")
      .option("sep", "\t")
      .option("escape", "\t")
      .csv("file:////home/ojuarezwork412/MovieLens/u.occupation")
      .toDF(occupations_columns: _*)
    //occupations.show(false)
    val users = sparkSession.read.format("csv").option("header", "false")
      .option("sep", "|")
      .option("escape", "\t")
      .csv("file:////home/ojuarezwork412/MovieLens/u.user")
      .toDF(user_columns: _*)
    //users.show(false)

    val genres = sparkSession.read.format("csv").option("header", "false")
      .option("sep", "\t")
      .option("escape", "\t")
      .csv("file:////home/ojuarezwork412/MovieLens/u.genre")
      .toDF(genres_columns: _*)


    //genres.show(10)
    //print(genres.show(false))
    //Transform _data from Unix Time Stamp
    val ratings_date=ratings.select(col("user_id"),col("item_id"),col("rating"),col("timestamp"),
      (from_unixtime(col("timestamp"))).as("timestamp5")
      , (year(from_unixtime(col("timestamp"))).as("year"))
      , (month(from_unixtime(col("timestamp"))).as("month"))
    )

   // ratings.printSchema()

    //val df11= df10.select(year(timestamp5).alias("year"),month(df10.timestamp5).alias("month"))
    //df11.show()
    //val joinCondition = movies.col("movie_id")
    val ratings_movie = ratings_date.join(movies,ratings_date("item_id") === movies("movie_id"),"left")
    print(ratings_movie.head())
    ratings_movie.show()
    val ratings_all = ratings_movie.groupBy("item_id").agg(count("item_id").alias("counter")).sort(desc("counter")).show()
    val ratings_year = ratings_movie.groupBy("item_id","year").count().sort().show()
    val ratings_year_month = ratings_movie.groupBy("item_id","year","month").count().sort().show()
    val unknownDF = ratings_movie.where(ratings_movie("unknown") ===1)
    unknownDF.groupBy("movie_title","year","month").agg(count("movie_title").alias("counter")).sort(desc("counter")).show()
    val ActionDF = ratings_movie.where(ratings_movie("Action") ===1)
    ActionDF.groupBy("movie_title","year","month").agg(count("movie_title").alias("counter")).sort(desc("counter")).show()
    /* vsl Acti
     val AdventureDF
     val AnimationDF
     val ChildrensDF
     val ComedyDF
     val CrimeDF
     val DocumentaryDF
     val DramaDF
     val FantasyDF
     val Film-NoirDF
     val HorrorDF
     val MusicalDF
     val MysteryDF
     val RomanceDF
     val Sci-FiDF
     val ThrillerDF
     val WarDF
     val WesternDF  */
    //#print(df3)

    sparkSession.stop()
  }
}
