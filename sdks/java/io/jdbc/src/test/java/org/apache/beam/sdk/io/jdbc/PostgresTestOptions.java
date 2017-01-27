package org.apache.beam.sdk.io.jdbc;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.testing.TestPipelineOptions;

/**
 * These options can be used by a test connecting to a postgres database to configure their connection.
 */
public interface PostgresTestOptions extends TestPipelineOptions {
    @Description("Server name for postgres server (host name/ip address)")
    @Default.String("postgres-server-name")
    String getPostgresServerName();
    void setPostgresServerName(String value);

    @Description("Username for postgres server")
    @Default.String("postgres-username")
    String getPostgresUsername();
    void setPostgresUsername(String value);

    // Note that passwords are not as secure an authentication as other methods, and used here for
    // a test environment only.
    @Description("Password for postgres server")
    @Default.String("postgres-password")
    String getPostgresPassword();
    void setPostgresPassword(String value);

    @Description("Database name for postgres server")
    @Default.String("postgres-database-name")
    String getPostgresDatabaseName();
    void setPostgresDatabaseName(String value);

    @Description("Port for postgres server")
    @Default.Integer(0)
    Integer getPostgresPort();
    void setPostgresPort(Integer value);

    @Description("Whether the postgres server uses SSL")
    @Default.Boolean(true)
    Boolean getPostgresSsl();
    void setPostgresSsl(Boolean value);
}
