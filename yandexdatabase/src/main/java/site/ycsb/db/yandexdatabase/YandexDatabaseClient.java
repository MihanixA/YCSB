/**
 * Copyright (c) 2020 YCSB contributors. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

package site.ycsb.db.yandexdatabase;

import site.ycsb.ByteIterator;
import site.ycsb.Client;
import site.ycsb.DB;
import site.ycsb.DBException;
import site.ycsb.StringByteIterator;
import site.ycsb.workloads.CoreWorkload;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.Nullable;
import javax.annotation.WillNotClose;

import com.yandex.ydb.core.Result;
import com.yandex.ydb.core.Status;
import com.yandex.ydb.core.rpc.RpcTransport;
import com.yandex.ydb.table.Session;
import com.yandex.ydb.table.TableClient;
import com.yandex.ydb.table.description.TableColumn;
import com.yandex.ydb.table.description.TableDescription;
import com.yandex.ydb.table.query.DataQuery;
import com.yandex.ydb.table.query.DataQueryResult;
import com.yandex.ydb.table.query.Params;
import com.yandex.ydb.table.result.ResultSetReader;
import com.yandex.ydb.table.rpc.grpc.GrpcTableRpc;
import com.yandex.ydb.table.transaction.Transaction;
import com.yandex.ydb.table.transaction.TransactionMode;
import com.yandex.ydb.table.transaction.TxControl;
import com.yandex.ydb.table.values.PrimitiveType;
import com.yandex.ydb.table.values.PrimitiveValue;



/**
 * @author MihanixA
 */

public class YandexDatabaseClient extends DB {

  public static final class YandexDatabaseProperties {
    private YandexDatabaseProperties() {}

    static final String DATABASE = "/ru-central1/b1g3vg66nr0frii1l327/etn012m0fmbjen0ro3re";

    static final String ENDPOINT = "lb.etn012m0fmbjen0ro3re.ydb.mdb.yandexcloud.net:2135";

    static final String SA_KEY_FILE = "keyfile";

    static final String ROOT_CERTIFICATES = "CA.pem";
  }

  private static int columnsCount;

  private static final int MAX_RETRIES = 5;

  private static final long OVERLOAD_DELAY_MILLIS = 5000;

  private static final Object LOCK = new Object();

  private static final Logger LOGGER = Logger.getLogger(YandexDatabaseClient.class.getName());

  private static final ArrayList<String> STANDARD_COLUMNS = new ArrayList<>();

  private String database;
  private TableClient tableClient;
  @Nullable
  private Session session;
  private Map<String, DataQuery> preparedQueries = new HashMap<>();

  public void init(@WillNotClose RpcTransport transport, String database) throws DBException {
    Properties properties = getProperties();

    synchronized (LOCK) {
      this.database = database;
      System.out.println(this.database);
      try {
        this.tableClient = TableClient.newClient(GrpcTableRpc.useTransport(transport))
            .build();

        this.session = tableClient.createSession()
            .join()
            .expect("cannot create session");
      } catch (Exception e) {
        throw new DBException(e);
      }
    }
    final String fieldPrefix = properties.getProperty(CoreWorkload.FIELD_NAME_PREFIX,
                                                      CoreWorkload.FIELD_NAME_PREFIX_DEFAULT);

    for (int i = 0; i < columnsCount; i++) {
      STANDARD_COLUMNS.add(fieldPrefix + i);
    }
  }

  @Override
  public void cleanup() throws DBException {
    try {
      if (session != null) {
        session.close()
            .join()
            .expect("cannot close session");
        tableClient.close();
      }
    } catch (Exception e) {
      LOGGER.log(Level.INFO, "cleanup failed", e);
      throw new DBException();
    }
  }

  @Override
  public site.ycsb.Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    Iterable<String> columns = fields == null ? STANDARD_COLUMNS : fields;
    final String queryId = "ReadQuery" + columns.toString(); // is it ordered ? is it safe ?

    try {

      DataQuery query = preparedQueries.get(queryId);

      if (query == null) {
        String queryText = String.format(
            "PRAGMA TablePathPrefix(\"%s\");\n" +
                "DECLARE $readKey as Utf8;\n" +
                "SELECT %s FROM %s WHERE Id=$readKey;",
            database, columns.toString(), table
        );

        query = executeWithResult(session -> session.prepareDataQuery(queryText).join());
        preparedQueries.put(queryId, query);
      }

      Params params = query.newParams()
          .put("$readKey", PrimitiveValue.utf8(key));
      DataQueryResult resultDataQuery = query.execute(TxControl.serializableRw().setCommitTx(true), params)
          .join()
          .expect("prepared query failed");

      if (resultDataQuery.isEmpty()) {
        throw new DBException("empty result");
      }

      ResultSetReader resultSet = resultDataQuery.getResultSet(0);
      resultSet.next();

      for (String col: columns) {
        result.put(col, new StringByteIterator(resultSet.getColumn(col).getUtf8()));
      }
      return site.ycsb.Status.OK;
    } catch (Exception e) {
      LOGGER.log(Level.INFO, "read query failed", e);
      return site.ycsb.Status.ERROR;
    }
  }

  @Override
  public site.ycsb.Status scan(String table, String startKey, int recordCount, Set<String> fields,
                     Vector<HashMap<String, ByteIterator>> result) {

    Iterable<String> columns = fields == null ? STANDARD_COLUMNS : fields;
    final String queryId = "ScanQuery" + columns.toString(); // is it ordered ?

    try {
      DataQuery query = preparedQueries.get(queryId);

      if (query == null) {
        String queryText = String.format(
            "PRAGMA TablePathPrefix(\"%s\");\n" +
                "DECLARE $startKey as Utf8;\n" +
                "DECLARE $limitCount as Uint64;\n" +
                "SELECT %s FROM %s WHERE Id>=$startKey\n" +
                "LIMIT $limitCount;",
            database, columns.toString(), table
        );

        query = executeWithResult(session -> session.prepareDataQuery(queryText).join());
        preparedQueries.put(queryId, query);
      }

      Params params = query.newParams()
          .put("$startKey", PrimitiveValue.utf8(startKey))
          .put("$limitCount", PrimitiveValue.uint64(recordCount));

      DataQueryResult resultDataQuery = query.execute(TxControl.serializableRw().setCommitTx(true), params)
          .join()
          .expect("prepared query failed");

      if (resultDataQuery.isEmpty()) {
        throw new DBException("empty result");
      }

      ResultSetReader resultSet = resultDataQuery.getResultSet(0);
      resultSet.next();
      while (!resultSet.isTruncated()) {
        resultSet.next();
        HashMap<String, ByteIterator> row = new HashMap<>();
        for (String col: columns) {
          row.put(col, new StringByteIterator(resultSet.getColumn(col).getUtf8()));
        }
        result.add(row); // possible memory leak
      }
      return site.ycsb.Status.OK;
    } catch (Exception e) {
      LOGGER.log(Level.INFO, "scan query failed", e);
      return site.ycsb.Status.ERROR;
    }
  }

  @Override
  public site.ycsb.Status update(String table, String key, Map<String, ByteIterator> values) {
    return site.ycsb.Status.OK;
  }

  @Override
  public site.ycsb.Status insert(String table, String key, Map<String, ByteIterator> values) {
    return site.ycsb.Status.OK;
  }

  @Override
  public site.ycsb.Status delete(String table, String key) {
    return site.ycsb.Status.OK;
  }

  /**
   * Executes given function with retry logic for YDB response statuses.
   *
   * In case of data transaction we have to retry the whole transaction as YDB
   * transaction may be invalidated on query error.
   *
   * @throws DBException in case if exception is not retryable or retry limit is exceeded
   */
  private com.yandex.ydb.core.Status execute(Function<Session, Status> fn) throws DBException {
    for (int i = 0; i < MAX_RETRIES; i++) {
      com.yandex.ydb.core.Status status = null;

      if (session == null) {
        // Session was invalidated, create new one here.
        // In real-world applications it's better to keep a pool of active sessions to avoid
        // additional latency on session creation.
        Result<Session> sessionResult = tableClient.createSession()
            .join();
        if (sessionResult.isSuccess()) {
          preparedQueries.clear();
          session = sessionResult.expect("cannot create session");
        } else {
          status = sessionResult.toStatus();
        }
      }

      if (session != null) {
        status = fn.apply(session);
        if (status.isSuccess()) {
          return status;
        }
      }

      assert status != null;

      switch (status.getCode()) {
        case ABORTED:
        case UNAVAILABLE:
          // Simple retry
          break;

        case OVERLOADED:
        case CLIENT_RESOURCE_EXHAUSTED:
          // Wait and retry. In applications with large parallelism it's better
          // to add some randomization to the delay to avoid congestion.
          try {
            Thread.sleep(OVERLOAD_DELAY_MILLIS);
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
          break;

        case NOT_FOUND:
          // May indicate invalidation of prepared query, clear prepared queries state
          preparedQueries.clear();
          break;

        case BAD_SESSION:
          // Session is invalidated, clear session state
          session = null;
          break;

        default:
          throw new DBException("non retryable exception");  // non retryable exception
      }
    }

    throw new DBException("too many retries"); // too many retries
  }

  private <T> T executeWithResult(Function<Session, Result<T>> fn) throws DBException {
    AtomicReference<Result<T>> result = new AtomicReference<>();
    com.yandex.ydb.core.Status status = execute(session -> {
      Result<T> r = fn.apply(session);
      result.set(r);
      return r.toStatus();
    });
    return result.get().expect("expected success result");
  }

}
