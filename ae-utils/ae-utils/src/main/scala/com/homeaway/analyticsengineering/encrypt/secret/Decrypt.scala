package com.homeaway.analyticsengineering.encrypt.secret

/**
  * Created by aguyyala on 6/21/17.
  */


import java.security.MessageDigest
import java.util
import javax.crypto.Cipher
import javax.crypto.spec.SecretKeySpec
import org.apache.commons.codec.binary.Base64
import java.sql.{Connection, DriverManager}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import scala.io.Source

private[encrypt] object Decrypt {

	private val log: Logger = Logger.getLogger(getClass)
	log.setLevel(Level.toLevel("Info"))

	private val credFileName = "credentials.properties"


	private def getCredentials(fName: String): Map[String, String] = {
		Source.fromFile(fName).getLines.map(_.split("=")).map { case Array(k, v) => (k, v) }.toMap
	}


	private final def getSparkConf(spark: SparkSession, appName: String, rdb: String, env: String, config: String, elseCase: String = null): String = {

		val conf =
			try {
				spark.conf.getOption(s"$appName.$rdb.$env.$config")
					.getOrElse(spark.conf.getOption(s"$appName.$rdb.$config")
						.getOrElse(spark.conf.getOption(s"$appName.$config")
							.getOrElse(spark.conf.getOption(s"$rdb.$env.$config")
								.getOrElse(spark.conf.getOption(s"$rdb.$config")
									.getOrElse(spark.conf.get(s"$config"))))))
			}

			catch {
				case _: NoSuchElementException => if (elseCase ne null) elseCase else throw new NoSuchElementException(config)
			}

		conf
	}


	private final def getRDBConf(spark: SparkSession, appName: String, rdb: String) = {
		val env = spark.conf.get("env")
		val vaultBaseUrl = getSparkConf(spark, appName, rdb, env, "vaultBaseUrl")
		val vaultAppUrl = getSparkConf(spark, appName, rdb, env, "vaultAppUrl")
		val vaultNonce = getSparkConf(spark, appName, rdb, env, "vaultNonce")
		val serverName = getSparkConf(spark, appName, rdb, env, "serverName")
		val port = getSparkConf(spark, appName, rdb, env, "port")
		val url = getSparkConf(spark, appName, rdb, env, "url", s"jdbc:$rdb://$serverName:$port")
		val user = getSparkConf(spark, appName, rdb, env, "user")
		val password = Vault.getPassword(vaultBaseUrl, vaultAppUrl, vaultNonce, serverName, user, env)

		(url, user, password)

	}


	final def getConn(spark: SparkSession, appName: String, rdb: String): Connection = {

		val (url, user, password) = getRDBConf(spark, appName, rdb)

		DriverManager.getConnection(url, user, password)
	}


	final def getSparkSession(ospark: SparkSession, appName: String, rdb: String): SparkSession = {

		val (url, user, password) = getRDBConf(ospark, appName, rdb)

		val spark = ospark.newSession()

		spark.conf.set("url", url)
		spark.conf.set("user", user)
		spark.conf.set("password", password)
		spark
	}


	final def getEncryptedPWD(actualPWD: String): String = Encryption.getEncryptedPWD(actualPWD)


	/**
	  * Sample:
	  * {{{
	  *   scala> val key = "private key!"
	  *
	  *   scala> Encryption.encrypt(key, "dummy")
	  *   res0: String = shjhdwwsdwd
	  *
	  *   scala> Encryption.decrypt(key", res0)
	  *   res1: String = dummy
	  * }}}
	  */
	private final object Encryption {

		private def encrypt(key: String, salt: String, value: String): String = {
			val cipher: Cipher = Cipher.getInstance("AES/ECB/PKCS5Padding")
			cipher.init(Cipher.ENCRYPT_MODE, keyToSpec(salt, key))
			Base64.encodeBase64String(cipher.doFinal(value.getBytes("UTF-8")))
		}

		private def decrypt(key: String, salt: String, encryptedValue: String): String = {
			val cipher: Cipher = Cipher.getInstance("AES/ECB/PKCS5PADDING")
			cipher.init(Cipher.DECRYPT_MODE, keyToSpec(salt, key))
			new String(cipher.doFinal(Base64.decodeBase64(encryptedValue)))
		}

		private def keyToSpec(salt: String, key: String): SecretKeySpec = {
			var keyBytes: Array[Byte] = (salt + key).getBytes("UTF-8")
			val sha: MessageDigest = MessageDigest.getInstance("SHA-1")
			keyBytes = sha.digest(keyBytes)
			keyBytes = util.Arrays.copyOf(keyBytes, 16)
			new SecretKeySpec(keyBytes, "AES")
		}


		def getEncryptedPWD(actualPWD: String): String = {

			encrypt(getCredentials(credFileName)("key"), getCredentials(credFileName)("salt"), actualPWD)
		}

		def getDecryptedPWD(encryptedPWD: String): String = {
			decrypt(getCredentials(credFileName)("key"), getCredentials(credFileName)("salt"), encryptedPWD)
		}

		def getEncryptedPWD(fName: String, actualPWD: String): String = {

			encrypt(getCredentials(fName)("key"), getCredentials(fName)("salt"), actualPWD)
		}

		def getDecryptedPWD(rdb: String, spark: SparkSession): String = {
			decrypt(spark.conf.get(s"$rdb.${spark.conf.get("env")}.key"), spark.conf.get(s"$rdb.${spark.conf.get("env")}.salt"), spark.conf.get(s"$rdb.${spark.conf.get("env")}.encryptedpassword"))
		}
	}

}
