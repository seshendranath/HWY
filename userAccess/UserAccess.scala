import java.io.FileWriter
import scala.util.parsing.json._
import scala.sys.process._
import language._

val accessToken = args(0)
val gitApiUrl = "https://github.company.com/api/v3"
val orgName = "AnalyticsEngineering"
val teamName = "Data Services"

/**
  * Add Users to Cluster
  *
  * @param user : username
  * @param keys : ssh keys of user
  * @return Unit
  */
def createUser(user: Any, keys: List[Any]): Unit = {

	val userexist = s"id -u $user" !

	if (userexist == 1) {

		s"useradd -m $user" !

		s"mkdir /home/$user/.ssh" !

		s"chmod 700 /home/$user/.ssh" !

		val fw = new FileWriter(s"""/home/$user/.ssh/authorized_keys""", true)

		keys.foreach { key => fw.write("\n" + key + "\n") }

		fw.close()

		s"chmod 600 /home/$user/.ssh/authorized_keys" !

		s"chown -R $user:$user /home/$user/.ssh" !

		print(s"user $user created")
	}
	else {
		print(s"user $user already exists")
	}
}


val teamString = s"curl -k $gitApiUrl/orgs/$orgName/teams?access_token=$accessToken" !!
val teamRecords = JSON.parseFull(teamString).get.asInstanceOf[Seq[Map[String, Any]]]
val teamUrl = teamRecords.filter(_ ("name") == teamName).head("url").toString

val memberString = s"curl -k $teamUrl/members?access_token=$accessToken" !!
val memberRecords = JSON.parseFull(memberString).get.asInstanceOf[Seq[Map[String, Any]]]
val members = memberRecords.map(x => x("login")).toList


for (user <- members) {
	val keyString = s"curl -k $gitApiUrl/users/$user/keys?access_token=$accessToken" !!
	val keyRecords = JSON.parseFull(keyString).get.asInstanceOf[Seq[Map[String, Any]]]
	val keys = keyRecords.map(x => x("key")).toList

	/* this checks user existance and creates user with ssh keys */
	createUser(user, keys)
}
