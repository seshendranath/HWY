import scala.sys.process._

val src=args(0)

val localDir = src.split("/").last

var query = ""
var out = ""


query = s"aws --profile ha-prod_dev s3 cp s3://$src $localDir --recursive"
println(s"Running $query")
out = query.!!
println(out)

query = s"""aws --profile ha-dev_dev s3 rm s3://${src.replaceAll("-prod-", "-dev-")} --recursive"""
println(s"Running $query")
out = query.!!
println(out)

query = s"""aws --profile ha-dev_dev s3 cp $localDir s3://${src.replaceAll("-prod-", "-dev-")} --recursive"""
println(s"Running $query")
out = query.!!
println(out)

query = s"rm -rf $localDir"
println(s"Running $query")
out = query.!!
println(out)
