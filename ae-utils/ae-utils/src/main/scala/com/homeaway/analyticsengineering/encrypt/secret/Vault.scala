package com.homeaway.analyticsengineering.encrypt.secret

/**
  * Created by aguyyala on 12/06/17.
  */


import java.security.cert.X509Certificate
import javax.net.ssl._
import org.codehaus.jettison.json.JSONObject
import java.net.{HttpURLConnection, URL}
import java.io.{BufferedWriter, OutputStreamWriter}
import org.apache.log4j.{Level, Logger}

object Vault {

	private val log: Logger = Logger.getLogger(getClass)
	log.setLevel(Level.toLevel("Info"))


	object TrustAll extends X509TrustManager {
		val getAcceptedIssuers: Array[X509Certificate] = null

		def checkClientTrusted(x509Certificates: Array[X509Certificate], s: String): Unit = {}

		def checkServerTrusted(x509Certificates: Array[X509Certificate], s: String): Unit = {}
	}


	// Verifies all host names by simply returning true.
	object VerifiesAllHostNames extends HostnameVerifier {
		def verify(s: String, sslSession: SSLSession) = true
	}


	val sslContext: SSLContext = SSLContext.getInstance("SSL")
	sslContext.init(null, Array(TrustAll), new java.security.SecureRandom())
	HttpsURLConnection.setDefaultSSLSocketFactory(sslContext.getSocketFactory)
	HttpsURLConnection.setDefaultHostnameVerifier(VerifiesAllHostNames)


	/**
	  * Returns the text (content) from a REST URL as a String.
	  *
	  * @param url            The full URL to connect to.
	  * @param connectTimeout Sets a specified timeout value, in milliseconds,
	  *                       to be used when opening a communications link to the resource referenced
	  *                       by this URLConnection. If the timeout expires before the connection can
	  *                       be established, a java.net.SocketTimeoutException
	  *                       is raised. A timeout of zero is interpreted as an infinite timeout.
	  *                       Defaults to 10000 ms.
	  * @param readTimeout    If the timeout expires before there is data available
	  *                       for read, a java.net.SocketTimeoutException is raised. A timeout of zero
	  *                       is interpreted as an infinite timeout. Defaults to 10000 ms.
	  * @param requestMethod  Defaults to "GET". (Other methods have not been tested.)
	  *
	  */
	def getPostContent(url: String, connectTimeout: Int = 10000, readTimeout: Int = 10000, requestMethod: String = "GET", httpHeaders: Map[String, String] = Map.empty[String, String], jsonBody: String = ""): String = {

		val connection = new URL(url).openConnection.asInstanceOf[HttpURLConnection]
		connection.setConnectTimeout(connectTimeout)
		connection.setReadTimeout(readTimeout)
		connection.setRequestMethod(requestMethod)
		httpHeaders.foreach(h => connection.setRequestProperty(h._1, h._2))
		if (jsonBody != "") {
			connection.setRequestMethod("POST")
			connection.setDoOutput(true)


			val out = new BufferedWriter(new OutputStreamWriter(connection.getOutputStream))
			out.write(jsonBody)
			out.flush()
			out.close()
		}
		//		val responseCode = connection.getResponseCode
		//		log.info("Sent: " + jsonBody + " to " + url + " received " + responseCode)
		val inputStream = connection.getInputStream
		val content = scala.io.Source.fromInputStream(inputStream).mkString
		if (inputStream != null) inputStream.close()
		content
	}


	def getEC2pkcs7: String = {
		val pkcs7 = getPostContent("http://169.254.169.254/latest/dynamic/instance-identity/pkcs7")
		pkcs7.split('\n').mkString
		pkcs7
	}


	def getEC2Role: String = {
		val role = getPostContent("http://169.254.169.254/latest/meta-data/iam/security-credentials/")
		role
	}


	def getVaultToken(vaultBaseUrl: String, vaultNonce: String): String = {
		val jsonBody = new JSONObject()
		jsonBody.put("role", getEC2Role)
		jsonBody.put("pkcs7", getEC2pkcs7)
		jsonBody.put("nonce", vaultNonce)
		val tokenJsonString = getPostContent(vaultBaseUrl + "/v1/auth/aws-ec2/login", 10000, 10000, "POST", Map("Content-Type" -> "application/x-www-form-urlencoded"), jsonBody.toString)
		val tokenJson = new JSONObject(tokenJsonString)
		val token = tokenJson.getJSONObject("auth").getString("client_token")
		token
	}


	def getPassword(vaultBaseUrl: String, vaultAppUrl: String, vaultNonce: String, serverName: String, userName: String, env: String = "prod"): String = {
		val token = getVaultToken(vaultBaseUrl, vaultNonce)
		val url = vaultBaseUrl + vaultAppUrl + env + "/awsregions/us-east-1/clusters/" + serverName + "/" + userName
		val credsString = getPostContent(url, 100000, 10000, "GET", Map("Content-Type" -> "application/x-www-form-urlencoded", "X-Vault-Token" -> token))
		val credJSON = new JSONObject(credsString)
		val cred = credJSON.getJSONObject("data").getString("password")
		cred
	}

}