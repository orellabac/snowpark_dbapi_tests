
-- Running queries from snowflake to public facing postgres database
-- using the public database at: https://rnacentral.org/help/public-database

-- You can retrieve the password from that link

-- Configure a secret.

CREATE  OR REPLACE SECRET pg_secret
TYPE = GENERIC_STRING
SECRET_STRING = $$
  {
    "user"    : "reader",
    "password": "<password>",
    "host"    : "hh-pgsql-public.ebi.ac.uk",
    "port"    : "5432",
    "database": "pfmegrnargs"
  }
  $$;


-- Configure a network rule.

CREATE OR REPLACE NETWORK RULE pg_network_rule
  MODE = EGRESS
  TYPE = HOST_PORT
  VALUE_LIST = ('hh-pgsql-public.ebi.ac.uk:5432');

-- Configure an external access integration.

CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION pg_access_integration
  ALLOWED_NETWORK_RULES = (pg_network_rule)
  ALLOWED_AUTHENTICATION_SECRETS = (pg_secret)
  ENABLED = true;


show integrations;



CREATE OR REPLACE FUNCTION READ_JDBC(OPTION OBJECT, query STRING) 
  RETURNS TABLE(data OBJECT)
  LANGUAGE JAVA
  RUNTIME_VERSION = '17'
  IMPORTS = ('@mystage/postgresql-42.7.7.jar')
  EXTERNAL_ACCESS_INTEGRATIONS = (pg_access_integration)
  SECRETS = ('cred' = pg_secret )
  HANDLER = 'JdbcDataReader'
AS $$
import java.sql.*;
import java.util.*;
import java.util.stream.Stream;
import com.snowflake.snowpark_java.types.SnowflakeSecrets;

public class JdbcDataReader {

    public static class OutputRow {
        public Map<String, String> data;

        public OutputRow(Map<String, String> data) {
            this.data = data;
        }
    }

    public static Class getOutputClass() {
      return OutputRow.class;
    }

    public Stream<OutputRow> process(Map<String, String> jdbcConfig, String query) {
        String jdbcUrl = jdbcConfig.get("url");
        String username;
        String password;
        
        if ("true".equals(jdbcConfig.get("use_secrets")))
        {
            SnowflakeSecrets sfSecrets = SnowflakeSecrets.newInstance();
            var secret = sfSecrets.getUsernamePassword("cred");
            username   = secret.getUsername();
            password   = secret.getPassword();
        }
        else 
        {
            username = jdbcConfig.get("username");
            password = jdbcConfig.get("password");
        }
        try {
            // Load the JDBC driver
            var driver = jdbcConfig.get("driver");
            if (driver != null)  Class.forName(driver);
            // Create a connection to the database
            Connection connection = DriverManager.getConnection(jdbcUrl, username, password);
            // Create a statement for executing SQL queries
            Statement statement = connection.createStatement();
            // Execute the query
            ResultSet resultSet = statement.executeQuery(query);
            // Get metadata about the result set
            ResultSetMetaData metaData = resultSet.getMetaData();
            // Create a list of column names
            List<String> columnNames = new ArrayList<>();
            int columnCount = metaData.getColumnCount();
            for (int i = 1; i <= columnCount; i++) {
                columnNames.add(metaData.getColumnName(i));
            }
            // Convert the ResultSet to a Stream of OutputRow objects
            Stream<OutputRow> resultStream = Stream.generate(() -> {
                try {
                    if (resultSet.next()) {
                        Map<String, String> rowMap = new HashMap<>();
                        for (String columnName : columnNames) {
                            String columnValue = resultSet.getString(columnName);
                            rowMap.put(columnName, columnValue);
                        }
                        return new OutputRow(rowMap);
                    } else {
                        // Close resources
                        resultSet.close();
                        statement.close();
                        connection.close();                        
                        return null;
                    }
                } catch (SQLException e) {
                    e.printStackTrace();
                    return null;
                }
            }).takeWhile(Objects::nonNull);
            return resultStream;
        } catch (Exception e) {
            e.printStackTrace();
            Map<String, String> rowMap = new HashMap<>();
            rowMap.put("ERROR",e.toString());
            return Stream.of(new OutputRow(rowMap));
        }
    }
}
$$;

select * from TABLE(READ_JDBC(
OBJECT_CONSTRUCT(
  --'driver','org.postgresql.Driver',
  'url','jdbc:postgresql://hh-pgsql-public.ebi.ac.uk/pfmegrnargs',
  'username', 'reader',
  'password', 'NWDMCE5xdipIjRrp'),
$$SELECT precomputed.id FROM rnc_rna_precomputed precomputed JOIN rnc_taxonomy tax ON tax.id = precomputed.taxid WHERE tax.lineage LIKE 'cellular organisms; Bacteria; %' AND precomputed.is_active = true AND rna_type = 'rRNA'$$
));

-- took aprox 2m 44 s


CREATE OR REPLACE PROCEDURE read_pg_dbapi(query string)
RETURNS TABLE()
LANGUAGE PYTHON
RUNTIME_VERSION='3.11'
HANDLER='run'
PACKAGES=('snowflake-snowpark-python', 'psycopg2')
EXTERNAL_ACCESS_INTEGRATIONS = (pg_access_integration, ALLOW_ALL_EAI)
SECRETS = ('cred' = pg_secret )
AS $$
import json
import _snowflake
from snowflake.snowpark import Session
conn_settings = {}
# Define the factory method for creating a connection to PostgreSQL
def create_pg_connection():
    import psycopg2
    #USER     = conn_settings.get("user")
    #PASSWORD = conn_settings.get("password")
    #HOST     = conn_settings.get("host")
    #PORT     = conn_settings.get("port")
    #DATABASE = conn_settings.get("database")  
    USER     = "reader"
    PASSWORD = "<password>"
    HOST     = "hh-pgsql-public.ebi.ac.uk"
    PORT     = "5432"
    DATABASE = "pfmegrnargs"
    connection = psycopg2.connect(
        host=HOST,
        port=PORT,
        dbname=DATABASE,
        user=USER,
        password=PASSWORD,
    )
    return connection

def run(session: Session, query:str):
    global conn_settings
    conn_settings = json.loads(_snowflake.get_generic_secret_string('cred'))
    df = session.read.dbapi(create_pg_connection,query=query)
    return df
$$;

CALL read_pg_dbapi($$ 
SELECT
    precomputed.id
FROM rnc_rna_precomputed precomputed
JOIN rnc_taxonomy tax
ON
    tax.id = precomputed.taxid
WHERE
    tax.lineage LIKE 'cellular organisms; Bacteria; %'
    AND precomputed.is_active = true    -- exclude sequences without active cross-references
    AND rna_type = 'rRNA'
$$);

-- ran for more than 5 minutes and I cancelled. 


