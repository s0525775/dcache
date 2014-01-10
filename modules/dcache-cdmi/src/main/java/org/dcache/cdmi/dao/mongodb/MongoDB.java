/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 * http://docs.mongodb.org/manual/tutorial/install-mongodb-on-red-hat-centos-or-fedora-linux/
 * http://api.mongodb.org/java/2.11.3/
 */

package org.dcache.cdmi.dao.mongodb;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.WriteResult;
import com.mongodb.util.JSON;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Jana
 */
public class MongoDB {

    private MongoClient mongo = null;
    private DB mongoDB = null;

    private static final String MONGO_SERVER = "localhost";
    private static final int MONGO_PORT = 27017;
    private static final String MONGO_USERNAME = "username";
    private static final String MONGO_PASSWORD = "password";

    public boolean connectWithoutDb() {
        boolean result = false;
        try {
            mongo = new MongoClient(MONGO_SERVER, MONGO_PORT);
            //MongoClient mongoClient = new MongoClient();
            //boolean auth = db.authenticate(MONGO_USERNAME, MONGO_PASSWORD.toCharArray());
            result = true;
        } catch (UnknownHostException ex) {
            Logger.getLogger(MongoDB.class.getName()).log(Level.SEVERE, null, ex);
        }
        return result;
    }

    public boolean connect(String database) {
        boolean result = false;
        try {
            mongo = new MongoClient(MONGO_SERVER, MONGO_PORT);
            mongoDB = mongo.getDB(database);
            //MongoClient mongoClient = new MongoClient();
            //boolean auth = db.authenticate(MONGO_USERNAME, MONGO_PASSWORD.toCharArray());
            result = true;
        } catch (UnknownHostException ex) {
            Logger.getLogger(MongoDB.class.getName()).log(Level.SEVERE, null, ex);
        }
        return result;
    }

    public void disconnect() {
        if (mongo != null) {
            mongo.close();
        }
    }

    public int countAllDB() {
	List<String> dbs = mongo.getDatabaseNames();
	return dbs.size();
    }

    public void printAllDB() {
	List<String> dbs = mongo.getDatabaseNames();
        System.out.println("List of all databases in Mongo:");
	for (String db : dbs) {
            System.out.println("- " + db);
	}
    }

    public List<String> getAllDB() {
	return mongo.getDatabaseNames();
    }

    public void dropDB(String database) {
	mongo.dropDatabase(database);
    }

    public int countAllTablesFromDB() {
	Set<String> tbls = mongoDB.getCollectionNames();
	return tbls.size();
    }

    public void printAllTablesFromDB() {
	Set<String> tbls = mongoDB.getCollectionNames();
        System.out.println("List of all tables in Mongo DB '" + mongoDB.getName() + "':");
	for (String tbl : tbls) {
            System.out.println("- " + tbl);
	}
    }

    public Set<String> getAllTablesFromDB() {
	return mongoDB.getCollectionNames();
    }

    public DBCollection getTableFromDB(String table) {
	return mongoDB.getCollection(table);
    }

    public void dropTable(String table) {
	DBCollection mongoTable = mongoDB.getCollection(table);
        mongoTable.drop();
    }

    public long countAllEntriesInTable(String table) {
	DBCollection mongoTable = mongoDB.getCollection(table);
        return mongoTable.getCount();
    }

    public WriteResult saveToDB(String table, DBObject object) {
        WriteResult result;
	DBCollection mongoTable = mongoDB.getCollection(table);
	result = mongoTable.insert(object);
        return result;
    }

    public WriteResult updateById(String table, String pnfsId, DBObject newObject) {
        WriteResult result;
	DBCollection mongoTable = mongoDB.getCollection(table);
	BasicDBObject query = new BasicDBObject();
	query.put("PnfsId", pnfsId);
	BasicDBObject updateObject = new BasicDBObject();
	updateObject.put("$set", newObject);
	result = mongoTable.update(query, updateObject);
        return result;
    }

    public WriteResult updateByObject(String table, DBObject oldObject, DBObject newObject) {
        WriteResult result;
	DBCollection mongoTable = mongoDB.getCollection(table);
	BasicDBObject query = new BasicDBObject();
	query.put("PnfsId", oldObject.get("PnfsId"));
	BasicDBObject updateObject = new BasicDBObject();
	updateObject.put("$set", newObject);
	result = mongoTable.update(query, updateObject);
        return result;
    }

    public DBObject fetchById(String table, String pnfsId) {
        DBObject result = null;
	DBCollection mongoTable = mongoDB.getCollection(table);
	BasicDBObject query = new BasicDBObject();
	query.put("PnfsId", pnfsId);
        try (DBCursor cursor = mongoTable.find(query)) {
            while (cursor.hasNext()) {
                result = cursor.next();
            }
            cursor.close();
        }
        return result;
    }

    public void fetchByIdAndPrint(String table, String pnfsId) {
	DBCollection mongoTable = mongoDB.getCollection(table);
	BasicDBObject query = new BasicDBObject();
	query.put("PnfsId", pnfsId);
        System.out.println("Object(s) in table '" + mongoTable.getName() + "' of Mongo database '" + mongoDB.getName() + "':");
        try (DBCursor cursor = mongoTable.find(query)) {
            while (cursor.hasNext()) {
                System.out.println("- " + cursor.next().toString());
            }
            cursor.close();
        }
    }

    public WriteResult deleteById(String table, String pnfsId) {
        WriteResult result;
	DBCollection mongoTable = mongoDB.getCollection(table);
	BasicDBObject searchQuery = new BasicDBObject();
	BasicDBObject query = new BasicDBObject();
	query.put("PnfsId", pnfsId);
 	result = mongoTable.remove(searchQuery);
        return result;
    }

    public DBObject convertJsonToDbObject(String jsonObject) {
        return (DBObject) JSON.parse(jsonObject);
    }

    public String convertDbObjectToJson(DBObject dbObject) {
        return (String) JSON.serialize(dbObject);
    }

    public long getMaxBsonObjectSize() {
        return mongo.getMaxBsonObjectSize();
    }

    public boolean isLocked() {
        return mongo.isLocked();
    }

    public void printMongoInformation() {
        System.out.println("Information about Mongo:");
        System.out.println("- Version: " + mongo.getVersion());
        System.out.println("- ConnectedPoint: " + mongo.getConnectPoint());
        System.out.println("- IsLocked: " + mongo.isLocked());
        System.out.println("- ConnectTimeout: " + mongo.getMongoClientOptions().getConnectTimeout());
        System.out.println("- ConnectionsPerHost: " + mongo.getMongoClientOptions().getConnectionsPerHost());
        System.out.println("- MaxBsonObjectSize: " + mongo.getMaxBsonObjectSize());
        System.out.println("- MaxAutoConnectRetryTime: " + mongo.getMongoClientOptions().getMaxAutoConnectRetryTime());
        System.out.println("- MaxWaitTime: " + mongo.getMongoClientOptions().getMaxWaitTime());
        System.out.println("- IsSocketKeepAlive: " + mongo.getMongoClientOptions().isSocketKeepAlive());
        System.out.println("- IsAutoConnectRetry: " + mongo.getMongoClientOptions().isAutoConnectRetry());
        System.out.println("- IsCursorFinalizerEnabled: " + mongo.getMongoClientOptions().isCursorFinalizerEnabled());
    }

}
