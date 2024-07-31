package com.demo;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import java.sql.*;

public class DataTransfer {
    public static void main (String[] args) {
        // MySQL's connection details (Source DB)
        String mysqlUrl = System.getenv( "MYSQL_URL" );
        String mysqlUser = System.getenv( "MYSQL_USER" );
        String mysqlPassword = System.getenv( "MYSQL_PASSWORD" );
        
        // PostgreSQL's connection details (Target DB)
        String postgresUrl = System.getenv( "POSTGRES_URL" );
        String postgresUser = System.getenv( "POSTGRES_USER" );
        String postgresPassword = System.getenv( "POSTGRES_PASSWORD" );
        
        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        
        try ( Connection mysqlConn = DriverManager.getConnection( mysqlUrl, mysqlUser, mysqlPassword );
              Connection postgresConn = DriverManager.getConnection( postgresUrl, postgresUser, postgresPassword ) ) {
            
            // Retrieve data from MySQL
            String selectSQL = "SELECT id AS journal_id, transactions FROM journal_vouchers WHERE YEAR(created_at) = " +
                    "2024";
            try ( Statement mysqlStatement = mysqlConn.createStatement();
                  ResultSet results = mysqlStatement.executeQuery( selectSQL ) ) {
                
                // Create table in PostgresSQL if it doesn't exist
                String createTableSQL = "CREATE TABLE IF NOT EXISTS temporary_particulars (" +
                        "id BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY," +
                        "journal_id BIGINT," +
                        "name VARCHAR(255)," +
                        "costCentre VARCHAR(255)," +
                        "rate VARCHAR(255)," +
                        "credit DOUBLE PRECISION," +
                        "debit DOUBLE PRECISION" +
                        ")";
                try ( Statement postgresStatement = postgresConn.createStatement() ) {
                    postgresStatement.execute( createTableSQL );
                } catch ( SQLException e ) {
                    throw new RuntimeException( e );
                }
                
                // Prepare insert statement for PostgreSQL
                String insertSQL = "INSERT INTO temporary_particulars (journal_id, name, costCentre, rate, " +
                        "credit, debit) VALUES ( ?, ?, ?, ?, ?, ?)";
                extractData( postgresConn, insertSQL, results, gson );
            }
        } catch ( SQLException e ) {
            System.out.println( e.getMessage() );
        }
    }
    
    private static void extractData (Connection postgresConn, String insertSQL, ResultSet results, Gson gson) throws SQLException {
        try ( PreparedStatement preparedStatement = postgresConn.prepareStatement( insertSQL ) ) {
            while ( results.next() ) {
                long journalId = results.getInt( "journal_id" );
                String transactionsJson = results.getString( "transactions" ).replace( "\\", "" ).replaceAll( "^\"|\"$", "" );
                
                JsonObject transactionsObject = gson.fromJson( transactionsJson, JsonObject.class );
                for ( String key : transactionsObject.keySet() ) {
                    JsonObject transaction = transactionsObject.getAsJsonObject( key );
                    String name = transaction.get( "particulars" ).getAsString();
                    String costCentre = getCostCentreStringOrNull( transaction );
                    String rateStr = transaction.get( "rate" ).getAsString();
                    String creditStr = transaction.get( "credit" ).getAsString();
                    String debitStr = transaction.get( "debit" ).getAsString();
                    
                    if(costCentre != null && !costCentre.isEmpty()) {
                        if ( costCentre.contains( "%" ) ){
                            costCentre = "NULL";
                        }
                    }
                    double credit = 0.0;
                    if ( creditStr != null ) {
                        credit = !creditStr.isEmpty() ? Double.parseDouble( creditStr ) : credit;
                    }
                    double debit = 0.0;
                    if ( debitStr != null ) {
                        debit = !debitStr.isEmpty() ? Double.parseDouble( debitStr ) : debit;
                    }
                    
                    preparedStatement.setLong( 1, journalId );
                    preparedStatement.setString( 2, name );
                    preparedStatement.setString( 3, costCentre != null && !costCentre.isEmpty() ? costCentre : null );
                    preparedStatement.setString( 4, rateStr );
                    preparedStatement.setDouble( 5, credit );
                    preparedStatement.setDouble( 6, debit );
                    
                    preparedStatement.addBatch();
                }
            }
            preparedStatement.executeBatch();
        }
    }
    
    private static String getCostCentreStringOrNull (JsonObject jsonObject) {
        JsonElement element = jsonObject.get( "costCenter" );
        if ( element != null && !element.isJsonNull() ) {
            return element.getAsString();
        } else {
            return null;
        }
    }
}