package io.exsql.mysql.server;

import rife.bld.Project;
import rife.bld.dependencies.Version;

import java.util.List;

import static rife.bld.dependencies.Repository.MAVEN_CENTRAL;
import static rife.bld.dependencies.Repository.RIFE2_RELEASES;
import static rife.bld.dependencies.Scope.*;

public class MysqlServerBuild extends Project {
    public MysqlServerBuild() {
        pkg = "io.exsql.mysql.server";
        name = "mysql-server";
        mainClass = "io.exsql.mysql.server.Bootstrap";
        version = version(0,1,0);

        downloadSources = true;
        repositories = List.of(MAVEN_CENTRAL, RIFE2_RELEASES);
        scope(compile)
                .include(dependency("io.projectreactor.netty", "reactor-netty", version(1,2,7)))
                .include(dependency("io.projectreactor.netty", "reactor-netty-http", version(1,2,7)))
                .include(dependency("org.slf4j", "slf4j-api", version(2, 0, 17)))
                .include(dependency("ch.qos.logback", "logback-core", version(1, 5, 18)))
                .include(dependency("ch.qos.logback", "logback-classic", version(1, 5, 18)))
                .include(dependency("io.netty", "netty-pkitesting", Version.parse("4.2.2.Final")))
                .include(dependency("org.bouncycastle", "bcprov-jdk18on", Version.parse("1.81")))
                .include(dependency("org.bouncycastle", "bcpkix-jdk18on", Version.parse("1.81")));

        scope(provided)
                .include(dependency("org.apache.spark", "spark-sql_2.13", version(4,0,0)));

        scope(test)
            .include(dependency("org.junit.jupiter", "junit-jupiter", version(5,11,4)))
            .include(dependency("org.junit.platform", "junit-platform-console-standalone", version(1,11,4)));
    }

    public static void main(String[] args) {
        new MysqlServerBuild().start(args);
    }
}