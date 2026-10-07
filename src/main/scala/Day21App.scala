import org.apache.spark.sql.SparkSession

object Day21App {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Day21-Spark-SQL-Catalog")
      .master("local[*]")
      .config("spark.ui.enabled", "false")
      .getOrCreate()

    import spark.implicits._

    println("===== DAY 21: SPARK SQL CATALOG =====")

    spark.sql("CREATE DATABASE IF NOT EXISTS analytics_db")
    spark.sql("USE analytics_db")

    println("DATABASES:")
    spark.sql("SHOW DATABASES").show(false)

    val employees = Seq(
      (1, "Mouaz", "Data Engineering", 70000),
      (2, "Rahul", "Data Engineering", 65000),
      (3, "Aman", "Analytics", 60000),
      (4, "Sara", "Analytics", 75000),
      (5, "John", "Cloud", 80000)
    ).toDF("employee_id", "name", "department", "salary")

    employees.createOrReplaceTempView("employees_view")

    println("TEMPORARY VIEWS:")
    spark.sql("SHOW VIEWS").show(false)

    println("SALARY > 65000:")
    spark.sql("""
      SELECT name, department, salary
      FROM employees_view
      WHERE salary > 65000
      ORDER BY salary DESC
    """).show(false)

    println("DEPARTMENT STATISTICS:")
    spark.sql("""
      SELECT
        department,
        COUNT(*) AS employees,
        ROUND(AVG(salary),2) AS avg_salary,
        MAX(salary) AS max_salary
      FROM employees_view
      GROUP BY department
      ORDER BY avg_salary DESC
    """).show(false)

    employees.write
      .mode("overwrite")
      .saveAsTable("analytics_db.employees")

    println("TABLES:")
    spark.sql("SHOW TABLES").show(false)

    println("TABLE DESCRIPTION:")
    spark.sql("DESCRIBE employees").show(false)

    spark.stop()
  }
}
