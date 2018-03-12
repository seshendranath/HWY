#!/usr/bin/env bash
// Run CMD: scala qubole_startup.scala $USER

import scala.util.Try
import scala.sys.process._
import java.io.FileWriter

val user=args(0)
val env=args(1)
val bashrcPath=s"/home/$user/.bashrc"
val bashProfilePath=s"/home/$user/.bash_profile"

val bashrc = 
"""
  |# .bashrc
  |
  |# Source global definitions
  |if [ -f /etc/bashrc ]; then
  |        . /etc/bashrc
  |fi
  |
  |# eval `keychain -q --eval id_rsa`
  |
  |function parse_git_branch () {
  |    git branch 2> /dev/null | sed -e '/^[^*]/d' -e 's/* \(.*\)/ (\1)/'
  |}
  |
  |
  |# User specific aliases and functions
  |source /usr/lib/hustler/bin/qubole-bash-lib.sh
""".stripMargin

new FileWriter(bashrcPath) { write(bashrc); close }

val bashProfile = 
"""
  |# .bash_profile
  |
  |# Get the aliases and functions
  |if [ -f ~/.bashrc ]; then
  |        . ~/.bashrc
  |fi
  |
  |
  |export LS_COLORS=$LS_COLORS:"di=00;33"
  |
  |
  |#######
  |
  |RED="\[\033[0;31m\]"
  |YELLOW="\[\033[0;33m\]"
  |GREEN="\[\033[0;32m\]"
  |NO_COLOUR="\[\033[0m\]"
  |export PS1="\${?} - $GREEN""".stripMargin + """\""" + """u@${HOSTNAME%%.*}$NO_COLOUR:\w$YELLOW\$(parse_git_branch)$NO_COLOUR\$ "
  |
  |alias gp="git pull"
  |alias gs="git status"
  |alias gs="git diff"
  |
  |# User specific environment and startup programs
  |
  |PATH=$PATH:$HOME/bin
  |
  |export SPARK_HOME=/usr/lib/qubole/packages/spark-2.2.0/spark
  |
  |PATH=$PATH:$SPARK_HOME/bin
  |
  |export PATH
  |
  |
  |alias hive="/usr/lib/qubole/packages/hive1_2-1.2/hive1.2/hive1.2"
  |
  |alias spark="spark-shell --master yarn --deploy-mode client --queue aeservices --executor-memory=15G --num-executors=70 --executor-cores=6 --driver-memory=10G --jars $(echo ~/jars/*.jar | tr ' ' ',') -i ~/jars/scalarc.scala" 
  |alias umspark="spark-shell --master yarn --deploy-mode client --queue aeservices --executor-memory=15G --num-executors=70 --executor-cores=10 --driver-memory=10G --jars $(echo ~/jars/*.jar | tr ' ' ',') -i ~/jars/scalarc.scala" 
  |alias mpark="spark-shell --master yarn --deploy-mode client --queue aeservices --executor-memory=15G --num-executors=50 --executor-cores=8 --driver-memory=10G --jars $(echo ~/jars/*.jar | tr ' ' ',') -i ~/jars/scalarc.scala" 
  |alias lspark="spark-shell --master yarn --deploy-mode client --queue aeservices --executor-memory=10G --num-executors=15 --executor-cores=7 --driver-memory=3G --jars $(echo ~/jars/*.jar | tr ' ' ',') -i ~/jars/scalarc.scala"
  |
  |export killspark="kill -9 \$(ps aux | grep \$(whoami) | grep 'spark-shell' | grep -v grep | awk '{print \$2}')"
  |alias ks="eval $killspark"
""".stripMargin

new FileWriter(bashProfilePath) { write(bashProfile); close }

"mkdir jars".!
s"aws s3 cp s3://{path} /home/$user/jars --recursive".!
