# sqlParser

## Usage
scala sqlParser.jar -i test.sql -o spark.sql

## Help
scala sqlParser.jar --help

## Jar Location 
https://github.homeawaycorp.com/aguyyala/sqlParser/tree/master/target/scala-2.12/
https://github.homeawaycorp.com/aguyyala/sqlParser/tree/master/target/scala-2.12/sqlParser.jar

It takes 2 mandatory arguments

1. -i | --input <Input_File_Path> -- This is the SqlServer's StoredProc Sql file to parse.
2. -o | --output <Output_File_Path> -- Once parsed it generates and writes the spark code to this output file.

Ex: Included test.sql (input file) and spark.sql (output file) in the project folder. You can view them to analyse what 
this SqlParser actually does.

## Json File Creation
If you want to create a Json File instead of a traditional spark sql file to plug into another framework like Aquila, you can run 
the below command to produce Json File. It will create Json File wherever (current) directory you are running the jar, also it uses
the same name as the input file except the file extension is .Json. You just to pass in --buildJson flag as an argument.

## Json Usage:
scala sqlParser.jar -i test.sql -o spark.sql --buildJson

The above command produces test.json file whereever you are running the Jar.


## Description
Sqlparser parses out sql file and extracts all the table and column information from it. Based on the retrieved information it 
tries to generate appropriate spark code which is useful for the EDW cloud migration project. It also cleans up all the comments
and replaces all the sqlServer keywords to spark keywords (ex: isnull -> coalesce). It can also parse the variables that are 
declared using declare statements in Sql file and replaces them where ever used in the queries (ex: if AppId is declared as 73)
it searches for all the @AppIds used in queries and replaces them.


## Pros
1. Saves developer's time and manual efforts to extract information.

## Cons
1. Not so perfect, you can consider it as pre-alpha release.
2. It may only work for certain use cases.
3. You have to still manually validate the code it generated.

