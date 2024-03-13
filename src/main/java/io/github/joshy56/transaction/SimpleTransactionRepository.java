package io.github.joshy56.transaction;

import co.aikar.idb.Database;
import co.aikar.idb.DbRow;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import io.github.joshy56.AbstractCachedRepository;
import io.github.joshy56.Namespace;
import io.github.joshy56.response.Response;
import org.intellij.lang.annotations.Language;
import org.jetbrains.annotations.NotNull;

import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * @author joshy56
 * @since 7/2/2024
 */
public class SimpleTransactionRepository extends AbstractCachedRepository<Namespace, Transaction> implements TransactionRepository {
    // Manejar las transacciones a la base de datos desde aqui, el cache.
    // Sera necesaria que una conexion sea inyectada por el constructor.
    @Language("RoomSql")
    private static final String SQL_SELECT = "SELECT identifier, amount FROM transactions WHERE identifier LIKE ?;", SQL_INSERT = "REPLACE INTO transactions(identifier, amount) VALUES(?, ?);", SQL_DELETE = "DELETE FROM transactions WHERE identifier LIKE ?;", SQL_CREATE_TABLE = "CREATE TABLE IF NOT EXISTS transactions(identifier VARCHAR(72) PRIMARY KEY NOT NULL, amount DOUBLE);";

    public SimpleTransactionRepository(Database database) {
        super(database, CacheBuilder.newBuilder().expireAfterAccess(3, TimeUnit.MINUTES).expireAfterWrite(1, TimeUnit.MINUTES).build(new CacheLoader<>() {
            @Override
            public @NotNull Transaction load(@NotNull Namespace namespace) throws Exception {
                DbRow dbRow = database.getFirstRow(SQL_SELECT, namespace.join());
                return Optional.ofNullable(dbRow).map(row -> {
                    double amount = row.getDbl("amount", 0);
                    return new Transaction(namespace.name(), UUID.fromString(namespace.key()), amount);
                }).orElseThrow(() -> new NoSuchElementException("No transaction present"));
            }
        }));

        query(statement -> {
            try {
                statement.query(SQL_CREATE_TABLE);
                statement.executeUpdate();

                statement.commit();

                return Response.empty();
            } catch (SQLException ok) {
                return Response.ofNullable(null, ok);
            }
        });
    }

    /**
     * @param namespace
     * @return
     */
    @Override
    public @NotNull Response<Transaction> get(@NotNull Namespace namespace) {
        Exception exception = null;
        Transaction value = null;
        try {
            value = cache().get(namespace);
        } catch (ExecutionException ok) {
            exception = ok;
        }
        return Response.ofNullable(value, exception);
    }

    /**
     * @param namespaces
     * @return
     */
    @Override
    public @NotNull Response<Set<Transaction>> getAllOfThem(@NotNull Set<Namespace> namespaces) {
        if (namespaces.isEmpty()) return Response.empty();
        return query(statement -> {
            Set<Transaction> value = null;
            Exception exception = null;
            try {
                statement.query("BEGIN TRANSACTION;");
                statement.executeUpdate();
                for (Namespace namespace : namespaces) {
                    statement.query(SQL_SELECT);
                    statement.execute(namespace.join());
                }
                statement.query("COMMIT;");
                statement.executeUpdate();

                statement.commit();

                value = statement.getResults().parallelStream().map(row -> {
                    UUID subjectId = UUID.fromString(row.getString("subjectId"));
                    String currencyName = row.getString("currencyName");
                    double amount = row.getDbl("amount", 0);
                    return new Transaction(currencyName, subjectId, amount);
                }).collect(Collectors.toSet());
            } catch (SQLException ok) {
                exception = ok;
            }
            return Response.ofNullable(value, exception);
        });
    }

    @Override
    public @NotNull Response<Set<Transaction>> getAll() {
        return query(statement -> {
            Set<Transaction> value = null;
            Exception exception = null;
            try {
                statement.query("SELECT identifier, amount FROM transactions;");
                statement.execute();

                statement.commit();

                value = statement.getResults().parallelStream().map(row -> {
                    UUID subjectId = UUID.fromString(row.getString("subjectId"));
                    String currencyName = row.getString("currencyName");
                    double amount = row.getDbl("amount", 0);
                    return new Transaction(currencyName, subjectId, amount);
                }).collect(Collectors.toSet());
            } catch (SQLException ok) {
                exception = ok;
            }
            return Response.ofNullable(value, exception);
        });
    }

    @Override
    public @NotNull Response<Set<Transaction>> getAllOfSubject(@NotNull UUID subjectId) {
        return query(statement -> {
            Set<Transaction> value = null;
            Exception exception = null;
            try {
                statement.query(SQL_SELECT);
                statement.execute("'" + subjectId + "%'");

                statement.commit();

                value = statement.getResults().parallelStream().map(row -> {
                    String currencyName = row.getString("currencyName");
                    double amount = row.getDbl("amount", 0);
                    return new Transaction(currencyName, subjectId, amount);
                }).collect(Collectors.toSet());
            } catch (SQLException ok) {
                exception = ok;
            }
            return Response.ofNullable(value, exception);
        });
    }

    @Override
    public @NotNull Response<Set<Transaction>> getAllOfCurrency(@NotNull String currencyName) {
        return query(statement -> {
            Set<Transaction> value = null;
            Exception exception = null;
            try {
                statement.query(SQL_SELECT);
                statement.execute("'%" + currencyName + "'");

                statement.commit();

                value = statement.getResults().parallelStream().map(row -> {
                    UUID subjectId = UUID.fromString(row.getString("subjectId"));
                    double amount = row.getDbl("amount", 0);
                    return new Transaction(currencyName, subjectId, amount);
                }).collect(Collectors.toSet());
            } catch (SQLException ok) {
                exception = ok;
            }
            return Response.ofNullable(value, exception);
        });
    }

    @Override
    public @NotNull Response<Void> delete(@NotNull Namespace namespace) {
        return query(statement -> {
            Exception exception = null;
            try {
                cache().invalidate(namespace);
                statement.query(SQL_DELETE);
                statement.executeUpdate(namespace.join());

                statement.commit();
            } catch (SQLException ok) {
                exception = ok;
            }
            return Response.ofNullable(null, exception);
        });
    }

    @Override
    public @NotNull Response<Void> deleteAllOfThem(@NotNull Set<Namespace> namespaces) {
        return query(statement -> {
            Exception exception = null;
            try {
                cache().invalidateAll(namespaces);
                statement.query("BEGIN TRANSACTION;");
                statement.executeUpdate();
                for (Namespace namespace : namespaces) {
                    statement.query(SQL_DELETE);
                    statement.executeUpdate(namespace.join());
                }
                statement.query("COMMIT;");
                statement.executeUpdate();

                statement.commit();
            } catch (SQLException ok) {
                exception = ok;
            }
            return Response.ofNullable(null, exception);
        });
    }

    @Override
    public @NotNull Response<Void> deleteAll() {
        return query(statement -> {
            Exception exception = null;
            try {
                cache().invalidateAll();
                statement.query("DELETE FROM transactions;");
                statement.executeUpdate();

                statement.commit();
            } catch (SQLException ok) {
                exception = ok;
            }
            return Response.ofNullable(null, exception);
        });
    }

    @Override
    public @NotNull Response<Void> set(@NotNull Transaction transaction) {
        return query(statement -> {
            Exception exception = null;
            try {
                Namespace namespace = new Namespace(transaction.subjectIdentifier().toString(), transaction.currencyName());

                statement.query(SQL_INSERT);
                statement.executeUpdate(namespace.join(), transaction.amount());

                statement.commit();

                if (cache().getIfPresent(namespace) != null) cache().put(namespace, transaction);
            } catch (SQLException ok) {
                exception = ok;
            }
            return Response.ofNullable(null, exception);
        });
    }

    /**
     * @param transactions
     * @return
     */
    @Override
    public @NotNull Response<Void> setAll(@NotNull Set<Transaction> transactions) {
        if (transactions.isEmpty()) return Response.empty();
        return query(statement -> {
            Exception exception = null;
            try {
                Map<Namespace, Transaction> replacement = new HashMap<>(transactions.size());
                Namespace namespace;
                statement.query("BEGIN TRANSACTION;");
                statement.executeUpdate();
                for (Transaction transaction : transactions) {
                    namespace = new Namespace(transaction.subjectIdentifier().toString(), transaction.currencyName());
                    statement.query(SQL_INSERT);
                    statement.executeUpdate(namespace.join(), transaction.amount());
                    if (cache().getIfPresent(namespace) != null) replacement.put(namespace, transaction);
                }
                statement.query("COMMIT;");
                statement.executeUpdate();

                statement.commit();

                cache().putAll(replacement);
            } catch (SQLException ok) {
                exception = ok;
            }
            return Response.ofNullable(null, exception);
        });
    }
}
