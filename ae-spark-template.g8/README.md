# Analytics Engineering Spark Template

## Description

This Spark Template is developed for Analytics Engineering as part of Architectural Standards. This template is easy to use and user friendly. It has all configurations, entry point, dependencies, utilities etc. built in, which saves ton of time for a developer in setting up a new project. This is used for creating SPARK jobs.

* Provides a readymade sparkSesion to use in other projects
* Preconfigured connections to HA hadoop, SQL cluster (which can be overridden)
* Contains common libraries, connectors and utilities
* Supports Scala 
* Builds in SBT
* JobControl, JobLogs etc are added in utilities to track your job status when it is running in cluster



## PreRequisites
* Should have sbt > 0.13.13 installed. Don't have sbt? No worries, [here is how to install sbt.](https://github.homeawaycorp.com/AnalyticsEngineering/ae-spark-template.g8#how-to-install-sbt)
* If you already have sbt installed, here is how to check the version of it.
  `sbt sbtVersion`
* If sbt version is <= 0.13.13, then run the following commands
  `brew update`
  `brew upgrade sbt` to upgrade your sbt version
* Should have Github Personal Access Token. Don't have the token? [here is how to generate it.](https://github.homeawaycorp.com/AnalyticsEngineering/ae-spark-template.g8#how-to-generate-github-personal-access-token)
* Add below code in target-relocate.sbt file (ex: ~/.sbt/1.0/target-relocate.sbt Or ~/.sbt/0.13/target-relocate.sbt) after setting up sbt. This will cleanup target folder litter.
```
import java.io.File

target := {
  val buildSbt = baseDirectory.value / "build.sbt"
  val projectSbt = baseDirectory.value / "project.sbt"
  val project = baseDirectory.value / "project"

  if (buildSbt.exists() || projectSbt.exists() || project.exists())
    target.value
  else
    new File("/tmp") / "sbt" / sbtVersion.value / "target"
}
```
* Make sure you have the following piece of code in your `~/.gitconfig` file otherwise you might encounter `cannot open git-upload-pack` error while running `sbt new`
```
[http]
    sslVerify = false
```


## Usage

* Run the following command in your terminal, NOTE: there is no .git at the end of URL.
```
sbt new https://github.homeawaycorp.com/AnalyticsEngineering/ae-spark-template.g8
```
* When prompted for Password, enter your Github Personal Access Token.
* It will prompt for `name (ex: DailyEmailSendfact_Load):`, this is your project name or more like the Folder name you want to create.
* It will prompt for `objectName (ex: marketing_activity_email_send_fact)`, this is your table name that goes into your application.properties and other files.
* That's it, your project folder is created and ready to use and modify.



## How to Install sbt?
### Prerequisites:
* Should have Homebrew installed. Don't have Homebrew? No worries, [here is how to install Homebrew.](https://github.homeawaycorp.com/AnalyticsEngineering/ae-spark-template.g8#how-to-install-homebrew)
* Run the following command.
```brew install sbt```



## How to Install Homebrew?
* Run the following command.

```/usr/bin/ruby -e "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/master/install)"```

* More details on https://brew.sh


## How to generate Github Personal Access Token?
* Click on the following link https://github.homeawaycorp.com/settings/tokens/new
* Enter Token Description (Ex: Read Access, Admin Access etc.)
* Select the scopes, the minimum is to select the `repo` scope.
* Click Generate Token.
* Copy the generated token and save it somewhere because you will be needing this token for all purposes like accessing Github from HTTPS, via APIs etc. Use this token instead of your regular password while accessing Github through HTTPS.
* That's it, there is your new Github Personal Access Token.



## Who Owns This?

This template is maintained by:

* team: analyticsengineering
* slack: #analytics-eng-support
* JIRA: Analytics Engineering (AE)
