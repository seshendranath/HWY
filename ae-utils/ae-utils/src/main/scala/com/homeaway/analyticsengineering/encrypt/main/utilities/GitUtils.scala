package com.homeaway.analyticsengineering.encrypt.main.utilities

import java.io.{ByteArrayOutputStream, File, PrintWriter}
import java.nio.file.Files
import org.eclipse.jgit.treewalk.filter.PathFilter
import org.eclipse.jgit.lib._
import org.apache.commons.io.FileUtils
import org.eclipse.jgit.api.Git
import org.eclipse.jgit.revwalk.{RevCommit, RevTree, RevWalk}
import org.eclipse.jgit.transport.UsernamePasswordCredentialsProvider
import org.eclipse.jgit.treewalk.TreeWalk
import org.apache.commons.codec.CharEncoding

class GitUtils {

	val file = new File(System.getProperty("user.home") + "/.gitconfig")

	if (!file.exists) {
		val writer = new PrintWriter(file)
		writer.println("[http]")
		writer.println("sslverify = false")
		writer.close()
	}

	val directory: File = Files.createTempDirectory("repo").toFile

	def cloneRepo(gitURI: String, authToken: String): Repository = {

		val git: Git = Git.cloneRepository.setURI(gitURI).setCredentialsProvider(new UsernamePasswordCredentialsProvider(authToken, "")).setDirectory(directory).call()

		val repo: Repository = git.getRepository

		repo
	}


	def getLastCommit(repo: Repository): ObjectId = {

		val lastCommitId: ObjectId = repo.resolve(Constants.HEAD)

		lastCommitId
	}


	def getContentsofFile(repo: Repository, lastCommitId: ObjectId, file: String): String = {
		val walk = new RevWalk(repo)
		val commit: RevCommit = walk.parseCommit(lastCommitId)

		val tree: RevTree = commit.getTree
		val treeWalk = new TreeWalk(repo)

		treeWalk.addTree(tree)
		treeWalk.setRecursive(true)
		treeWalk.setFilter(PathFilter.create(file))

		if (!treeWalk.next) throw new IllegalStateException(s"Did not find expected file $file")


		val objectId: ObjectId = treeWalk.getObjectId(0)
		val loader: ObjectLoader = repo.open(objectId)

		walk.dispose()

		val baos = new ByteArrayOutputStream()
		loader.copyTo(baos)

		val contents = new String(baos.toByteArray, CharEncoding.UTF_8)
		contents
	}


	def cleanup(repo: Repository): Unit = {
		repo.close()
		FileUtils.forceDelete(directory)
	}
}
