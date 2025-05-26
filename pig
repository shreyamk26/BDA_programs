// data.txt
John,25,New York
Alice,30,Los Angeles
Bob,35,Chicago
Eve,28,Boston
Mark,40,Miami
//data1.txt
1,John
2,Alice
3,Bob
4,Eve
5,Mark
//data2.txt
1,25
2,30
3,35
4,28
5,40



data = LOAD 'data.txt' USING PigStorage(',') AS (name:chararray, age:int, city:chararray);
sorted_data = ORDER data BY age DESC;
grouped_data = GROUP data BY age;
data1 = LOAD 'data1.txt' USING PigStorage(',') AS (id:int, name:chararray);
data2 = LOAD 'data2.txt' USING PigStorage(',') AS (id:int, age:int);
joined_data = JOIN data1 BY id, data2 BY id;
projected_data = FOREACH data GENERATE name, age;
filtered_data = FILTER data BY age > 30;
STORE sorted_data INTO 'sorted_data_output' USING PigStorage(',');
STORE grouped_data INTO 'grouped_data_output' USING PigStorage(',');
STORE joined_data INTO 'joined_data_output' USING PigStorage(',');
STORE projected_data INTO 'projected_data_output' USING PigStorage(',');
STORE filtered_data INTO 'filtered_data_output' USING PigStorage(',');

pig -x local script.pig
