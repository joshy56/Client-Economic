package io.github.joshy56.transaction;

import co.aikar.idb.Database;
import co.aikar.idb.DbRow;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import io.github.joshy56.AbstractCachedRepository;
import io.github.joshy56.Namespace;
import io.github.joshy56.response.Response;
import io.github.joshy56.response.ResponseCode;
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

    public SimpleTransactionRepository(Database database) {
        super(database, CacheBuilder.newBuilder().expireAfterAccess(3, TimeUnit.MINUTES).expireAfterWrite(1, TimeUnit.MINUTES).build(
                new CacheLoader<>() {
                    @Override
                    public @NotNull Transaction load(@NotNull Namespace namespace) throws Exception {
                        DbRow dbRow = database.getFirstRow("SELECT subjectId, currencyName, amount FROM transactions WHERE subjectId=? AND currencyName=?;", namespace.key(), namespace.name());
                        return Optional.ofNullable(dbRow)
                                .map(row -> {
                                    UUID subjectId = UUID.fromString(row.getString("subjectId"));
                                    double amount = row.getDbl("amount", 0);
                                    return new Transaction(namespace.name(), subjectId, amount);
                                }).orElseThrow(() -> new NullPointerException("Transaction don't exists."));
                    }
                }
        ));
    }

    /**
     * @param namespace
     * @return
     */
    @Override
    public @NotNull Response<Transaction> get(@NotNull Namespace namespace) {
        try {
            Transaction transaction = cache().get(namespace);
            return new Response<>(ResponseCode.OK, Optional.empty(), Optional.of(transaction));
        } catch (ExecutionException ok) {
            return new Response<>(ResponseCode.ERROR, Optional.of(new RuntimeException("Something got wrong, ups.", ok)), Optional.empty());
        }
    }

    /**
     * @param namespaces
     * @return
     */
    @Override
    public @NotNull Response<Set<Transaction>> getAllOfThem(@NotNull Set<Namespace> namespaces) {
        if (namespaces.isEmpty()) return new Response<>(ResponseCode.OK, Optional.empty(), Optional.empty());
        return query(statement -> {
            try {
                statement.query("BEGIN TRANSACTION;");
                statement.execute();
                for (Namespace namespace : namespaces) {
                    statement.query("SELECT subjectId, currencyName, amount FROM transactions WHERE subjectId=? AND currencyName=?;");
                    statement.execute(namespace.key(), namespace.name());
                }
                statement.query("COMMIT;");
                statement.execute();

                statement.commit();

                Set<Transaction> transactions = statement.getResults().parallelStream().map(row -> {
                    UUID subjectId = UUID.fromString(row.getString("subjectId"));
                    String currencyName = row.getString("currencyName");
                    double amount = row.getDbl("amount", 0);
                    return new Transaction(currencyName, subjectId, amount);
                }).collect(Collectors.toSet());
                return new Response<>(ResponseCode.OK, Optional.empty(), Optional.ofNullable(transactions.isEmpty() ? null : transactions));
            } catch (SQLException ok) {
                return new Response<>(ResponseCode.ERROR, Optional.of(new RuntimeException("", ok)), Optional.empty());
            }
        });
    }

    @Override
    public @NotNull Response<Set<Transaction>> getAll() {
        return query(statement -> {
            try {
                statement.query("SELECT subjectId, currencyName, amount FROM transactions;");
                statement.execute();

                statement.commit();

                Set<Transaction> transactions = statement.getResults().parallelStream().map(row -> {
                    UUID subjectId = UUID.fromString(row.getString("subjectId"));
                    String currencyName = row.getString("currencyName");
                    double amount = row.getDbl("amount", 0);
                    return new Transaction(currencyName, subjectId, amount);
                }).collect(Collectors.toSet());
                return new Response<>(ResponseCode.OK, Optional.empty(), Optional.ofNullable(transactions.isEmpty() ? null : transactions));
            } catch (SQLException ok) {
                return new Response<>(ResponseCode.ERROR, Optional.of(ok), Optional.empty());
            }
        });
    }

    @Override
    public @NotNull Response<Set<Transaction>> getAllOfSubject(@NotNull UUID subjectId) {
        return query(statement -> {
            try {
                statement.query("SELECT subjectId, currencyName, amount FROM transactions WHERE subjectId=?;");
                statement.execute(subjectId);

                statement.commit();

                Set<Transaction> transactions = statement.getResults().parallelStream().map(row -> {
                    String currencyName = row.getString("currencyName");
                    double amount = row.getDbl("amount", 0);
                    return new Transaction(currencyName, subjectId, amount);
                }).collect(Collectors.toSet());
                if (transactions.isEmpty()) return new Response<>(ResponseCode.OK, Optional.empty(), Optional.empty());
                return new Response<>(ResponseCode.OK, Optional.empty(), Optional.of(transactions));
            } catch (SQLException ok) {
                return new Response<>(ResponseCode.ERROR, Optional.of(new RuntimeException("Palta, why not?", ok)), Optional.empty());
            }
        });
    }

    @Override
    public @NotNull Response<Set<Transaction>> getAllOfCurrency(@NotNull String currencyName) {
        return query(statement -> {
            try {
                statement.query("SELECT subjectId, currencyName, amount FROM transactions WHERE currencyName=?;");
                statement.execute(currencyName);

                statement.commit();

                Set<Transaction> transactions = statement.getResults().parallelStream().map(row -> {
                    UUID subjectId = UUID.fromString(row.getString("subjectId"));
                    double amount = row.getDbl("amount", 0);
                    return new Transaction(currencyName, subjectId, amount);
                }).collect(Collectors.toSet());
                if (transactions.isEmpty()) return new Response<>(ResponseCode.OK, Optional.empty(), Optional.empty());
                return new Response<>(ResponseCode.OK, Optional.empty(), Optional.of(transactions));
            } catch (SQLException ok) {
                return new Response<>(ResponseCode.ERROR, Optional.of(new RuntimeException("Changoos.", ok)), Optional.empty());
            }
        });
    }

    @Override
    public @NotNull Response<Void> delete(@NotNull Namespace namespace) {
        return query(statement -> {
            try {
                cache().invalidate(namespace);
                statement.query("DELETE FROM transactions WHERE subjectId=? AND currencyName=?;");
                statement.execute(namespace.key(), namespace.name());

                statement.commit();

                return new Response<>(ResponseCode.OK, Optional.empty(), Optional.empty());
            } catch (SQLException ok) {
                return new Response<>(ResponseCode.ERROR, Optional.of(ok), Optional.empty());
            }
        });
    }

    @Override
    public @NotNull Response<Void> deleteAllOfThem(@NotNull Set<Namespace> namespaces) {
        return query(statement -> {
            try {
                cache().invalidateAll(namespaces);
                statement.query("BEGIN TRANSACTION;");
                statement.execute();
                for (Namespace namespace : namespaces) {
                    statement.query("DELETE FROM transactions WHERE subjectId=? AND currencyName=?;");
                    statement.execute(namespace.key(), namespace.name());
                }
                statement.query("COMMIT;");
                statement.execute();

                statement.commit();

                return new Response<>(ResponseCode.OK, Optional.empty(), Optional.empty());
            } catch (SQLException ok) {
                return new Response<>(ResponseCode.ERROR, Optional.of(ok), Optional.empty());
            }
        });
    }

    @Override
    public @NotNull Response<Void> deleteAll() {
        return query(statement -> {
            try {
                cache().invalidateAll();
                statement.query("DELETE FROM transactions;");
                statement.execute();

                statement.commit();

                return new Response<>(ResponseCode.OK, Optional.empty(), Optional.empty());
            } catch (SQLException ok) {
                return new Response<>(ResponseCode.ERROR, Optional.of(ok), Optional.empty());
            }
        });
    }

    @Override
    public @NotNull Response<Void> set(@NotNull Transaction transaction) {
        return query(statement -> {
            try {
                statement.query("INSERT INTO transactions(subjectId, currencyName, amount) VALUES(?, ?, ?) ON CONFLICT(subjectId, currencyName) DO UPDATE SET amount=?;");
                statement.execute(transaction.subjectIdentifier(), transaction.currencyName(), transaction.amount(), transaction.amount());

                statement.commit();

                Namespace namespace = new Namespace(transaction.subjectIdentifier().toString(), transaction.currencyName());
                if (cache().getIfPresent(namespace) != null) cache().put(namespace, transaction);
                return new Response<>(ResponseCode.OK, Optional.empty(), Optional.empty());
            } catch (SQLException ok) {
                return new Response<>(ResponseCode.ERROR, Optional.of(ok), Optional.empty());
            }
        });
    }

    /**
     * @param transactions
     * @return
     */
    @Override
    public @NotNull Response<Void> setAll(@NotNull Set<Transaction> transactions) {
        if (transactions.isEmpty()) return new Response<>(ResponseCode.OK, Optional.empty(), Optional.empty());
        return query(statement -> {
            try {
                Map<Namespace, Transaction> replacement = new HashMap<>(transactions.size());
                Namespace namespace;
                statement.query("BEGIN TRANSACTION;");
                statement.execute();
                for (Transaction transaction : transactions) {
                    statement.query("INSERT INTO transactions(subjectId, currencyName, amount) VALUES(?, ?, ?) ON CONFLICT(subjectId, currencyName) DO UPDATE SET amount=?;");
                    statement.execute(transaction.subjectIdentifier(), transaction.currencyName(), transaction.amount(), transaction.amount());
                    namespace = new Namespace(transaction.subjectIdentifier().toString(), transaction.currencyName());
                    if (cache().getIfPresent(namespace) != null) replacement.put(namespace, transaction);
                }
                statement.query("COMMIT;");
                statement.execute();

                statement.commit();

                cache().putAll(replacement);
                return new Response<>(ResponseCode.OK, Optional.empty(), Optional.empty());
            } catch (SQLException ok) {
                return new Response<>(ResponseCode.ERROR, Optional.of(ok), Optional.empty());
            }
        });
    }
}
