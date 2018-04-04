require "minitest/autorun"

class TestBasicFunctionality < Minitest::Test
  def test_hdfs
    system "docker exec -it spark bash -c \"echo abc123 > testfile\""
    assert_equal $?.exitstatus, 0

    match_found = `docker exec -it spark bash -c \"hdfs dfs -ls / | grep testfile | wc -l\"`
    assert match_found.to_i > 0

    system "docker exec -it spark bash -c \"hdfs dfs -put -f testfile /\""
    assert_equal $?.exitstatus, 0

    match_found = `docker exec -it spark bash -c \"hdfs dfs -cat /testfile | grep abc123 | wc -l\"`
    assert match_found.to_i > 0
  end

  def test_yarn
    output = `docker exec -it spark bash -c \"yarn jar /opt/hadoop/hadoop-mapreduce-project/hadoop-mapreduce-examples/target/hadoop-mapreduce-examples-2.7.4.jar pi 10 100\"`
    match_found = output.scan /Estimated value of Pi is 3.14/
    assert match_found.length == 1
  end

  def test_hive
    File.open ".test.hql", File::RDWR|File::CREAT, 0644 do |f|
      f.truncate 0
      f.write 'CREATE DATABASE IF NOT EXISTS hivetest; CREATE TABLE IF NOT EXISTS hivetest.students (name VARCHAR(64), age INT, gpa DECIMAL(3, 2)) CLUSTERED BY (age) INTO 2 BUCKETS STORED AS ORC; INSERT INTO TABLE hivetest.students VALUES (\'fred flintstone\', 35, 1.28), (\'barney rubble\', 32, 2.32); SELECT * FROM hivetest.students;'
    end

    system "docker cp .test.hql hive:/root/"
    assert_equal $?.exitstatus, 0

    output = `docker exec -it hive bash -c \"hive -f .test.hql\"`
    assert output.scan(/fred flintstone\s+35\s+1.28/).length > 0
    assert output.scan(/barney rubble\s+32\s+2.32/).length > 0

    system "rm -f .test.hql"
    assert_equal $?.exitstatus, 0
  end

  def test_spark
    output = `docker exec -it spark bash -c \"/opt/spark/bin/spark-submit \
              --class org.apache.spark.examples.SparkPi \
              --master yarn \
              --deploy-mode cluster \
              --executor-memory 512m \
              --num-executors 1 \
              /opt/spark/examples/target/original-spark-examples_2.11-2.3.0-SNAPSHOT.jar \
              1\"`
    match_found = output.scan /final status: SUCCEEDED/
    assert match_found.length == 1
  end
end
