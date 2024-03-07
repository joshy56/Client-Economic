package io.github.joshy56.transaction;

import co.aikar.idb.Database;
import co.aikar.idb.DbRow;
import co.aikar.idb.DbStatement;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import io.github.joshy56.AbstractCachedRepository;
import io.github.joshy56.Economic;
import io.github.joshy56.Namespace;
import io.github.joshy56.currency.Currency;
import io.github.joshy56.currency.CurrencyRepository;
import io.github.joshy56.response.Response;
import io.github.joshy56.response.ResponseCode;
import io.github.joshy56.subject.Subject;
import io.github.joshy56.subject.SubjectRepository;
import io.github.joshy56.transaction.Transaction;
import io.github.joshy56.transaction.TransactionRepository;
import org.bukkit.plugin.java.JavaPlugin;
import org.jetbrains.annotations.NotNull;

import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

/**
 * @author joshy56
 * @since 7/2/2024
 */
public class SimpleTransactionRepository extends AbstractCachedRepository<Namespace, Transaction> implements TransactionRepository {
    // Manejar las transacciones a la base de datos desde aqui, el cache.
    // Sera necesaria que una conexion sea inyectada por el constructor.
    private final LoadingCache<String, Double> transactionCache;
    private final Economic economic;
    private final Database db;

    public SimpleTransactionRepository(Economic economic, JavaPlugin plugin, Database database) {
        super(CacheBuilder.newBuilder().expireAfterAccess(3, TimeUnit.MINUTES).expireAfterWrite(1, TimeUnit.MINUTES).build(
                new CacheLoader<>() {
                    @Override
                    public Transaction load(Namespace namespace) throws Exception {
                        DbRow row = database.getFirstRow("SELECT amount FROM transactions WHERE subjectId=? AND currencyName=?", namespace.key(), namespace.name());
                        return new Transaction(namespace.name(), UUID.fromString(namespace.key()), row.getDbl("amount"));
                    }
                }
        ));
        this.economic = economic;
        this.db = database;
        transactionCache = CacheBuilder.newBuilder().expireAfterAccess(3, TimeUnit.MINUTES).expireAfterWrite(1, TimeUnit.MINUTES).build(
                new CacheLoader<>() {
                    @Override
                    public Double load(String key) throws Exception {
                        String[] keypair = key.split(":");
                        UUID.fromString(keypair[0]);
                        DbRow row = db.getFirstRow("SELECT amount FROM transaction WHERE subjectId=?, currencyName=?;", keypair[0], keypair[1]);
                        return row.getDbl("amount");
                    }
                }
        );
    }

    @Override
    public Response<Transaction> get(UUID subjectIdentifier, String currencyName) {
        try {
            double amount = transactionCache.get(subjectIdentifier.toString().concat(":").concat(currencyName.toLowerCase()));
            return new Response<>(ResponseCode.OK, Optional.empty(), Optional.of(new Transaction(currencyName, subjectIdentifier, amount)));
        } catch (ExecutionException ok) {
            return new Response<>(ResponseCode.ERROR, Optional.of(ok), Optional.empty());
        }
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
     * @param set
     * @return
     */
    @Override
    public @NotNull Response<Set<Transaction>> getAllOfThem(@NotNull Set<Namespace> set) {
        return query(db, (BiFunction<DbStatement, LoadingCache<Namespace, Transaction>, Response<Set<Transaction>>>) (dbStatement, namespaceTransactionLoadingCache) -> {
            try {
                dbStatement.query("");
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Override
    public Response<Set<Transaction>> getAll() {
        try (DbStatement statement = db.query("SELECT subjectId, currencyName, amount FROM transaction;")) {
            Set<Transaction> transactions = statement.getResults().parallelStream().map(row -> {
                UUID subjectId = UUID.fromString(row.getString("subjectId"));
                return new Transaction(row.getString("currencyName"), subjectId, row.getDbl("amount"));
            }).collect(Collectors.toSet());
            if (transactions.isEmpty()) return new Response<>(ResponseCode.OK, Optional.empty(), Optional.empty());
            return new Response<>(ResponseCode.OK, Optional.empty(), Optional.of(transactions));
        } catch (SQLException | IllegalArgumentException ok) {
            return new Response<>(ResponseCode.ERROR, Optional.of(ok), Optional.empty());
        }
    }

    @Override
    public Response<Set<Transaction>> getAllOfSubject(UUID subjectId) {
        try (DbStatement statement = db.query("SELECT currencyName, amount FROM transaction WHERE subjectId=?;")) {
            Set<Transaction> transactions = statement.execute(subjectId.toString()).getResults().parallelStream().map(row -> new Transaction(row.getString("currencyName"), subjectId, row.getDbl("amount"))).collect(Collectors.toSet());
            if (transactions.isEmpty()) return new Response<>(ResponseCode.OK, Optional.empty(), Optional.empty());
            return new Response<>(ResponseCode.OK, Optional.empty(), Optional.of(transactions));
        } catch (SQLException ok) {
            return new Response<>(ResponseCode.ERROR, Optional.of(ok), Optional.empty());
        }
    }

    @Override
    public Response<Set<Transaction>> getAllOfCurrency(String currencyName) {
        try (DbStatement statement = db.query("SELECT subjectId, amount FROM transaction WHERE currencyName=?;")) {
            Set<Transaction> transactions = statement.execute(currencyName).getResults().parallelStream().map(row -> {
                UUID subjectId = UUID.fromString(row.getString("subjectId"));
                return new Transaction(currencyName, subjectId, row.getDbl("amount"));
            }).collect(Collectors.toSet());
            if (transactions.isEmpty()) return new Response<>(ResponseCode.OK, Optional.empty(), Optional.empty());
            return new Response<>(ResponseCode.OK, Optional.empty(), Optional.of(transactions));
        } catch (SQLException | IllegalArgumentException ok) {
            return new Response<>(ResponseCode.ERROR, Optional.of(ok), Optional.empty());
        }
    }

    @Override
    public Response<Void> delete(Namespace namespace) {
        try (DbStatement statement = db.createStatement()) {
            try {
                statement.startTransaction();
                if (!statement.inTransaction())
                    return new Response<>(ResponseCode.ERROR, Optional.of(new IllegalStateException("Currency delete operation require transaction.")), Optional.empty());

                statement.executeUpdateQuery("DELETE FROM transaction WHERE subjectId=? AND currencyName=?;", namespace.key(), namespace.name());
                statement.commit();

                transactionCache.invalidate(namespace.join());
                return new Response<>(ResponseCode.OK, Optional.empty(), Optional.empty());
            } catch (SQLException ok) {
                statement.rollback();
                return new Response<>(ResponseCode.ERROR, Optional.of(ok), Optional.empty());
            }
        } catch (SQLException ok) {
            return new Response<>(ResponseCode.ERROR, Optional.of(ok), Optional.empty());
        }
    }

    @Override
    public Response<Void> deleteAllOfThem(Set<Namespace> transactions) {
        if (transactions.isEmpty()) return new Response<>(ResponseCode.OK, Optional.empty(), Optional.empty());
        try (DbStatement statement = db.createStatement()) {
            try {
                statement.startTransaction();
                if (!statement.inTransaction())
                    return new Response<>(ResponseCode.ERROR, Optional.of(new IllegalStateException("Currency delete operation require transaction.")), Optional.empty());

                Set<String> keys = new HashSet<>(transactions.size());
                for (Transaction transaction : transactions) {
                    statement.executeUpdateQuery("DELETE FROM transaction WHERE subjectId=? AND currencyName=?", transaction.subjectIdentifier().toString(), transaction.currencyName());
                    keys.add(transaction.subjectIdentifier().toString().concat(":").concat(transaction.currencyName()));
                }
                statement.commit();

                transactionCache.invalidateAll(keys);
                return new Response<>(ResponseCode.OK, Optional.empty(), Optional.of(true));
            } catch (SQLException ok) {
                statement.rollback();
                return new Response<>(ResponseCode.ERROR, Optional.of(ok), Optional.of(false));
            }
        } catch (SQLException ok) {
            return new Response<>(ResponseCode.ERROR, Optional.of(ok), Optional.of(false));
        }
    }

    @Override
    public Response<Void> deleteAll() {
        try (DbStatement statement = db.createStatement()) {
            try {
                statement.startTransaction();
                if (!statement.inTransaction())
                    return new Response<>(ResponseCode.ERROR, Optional.of(new IllegalStateException("Currency delete operation require transaction.")), Optional.empty());

                statement.executeUpdateQuery("DELETE FROM transaction;");
                statement.commit();

                transactionCache.invalidateAll();
                return new Response<>(ResponseCode.OK, Optional.empty(), Optional.empty());
            } catch (SQLException ok) {
                statement.rollback();
                return new Response<>(ResponseCode.ERROR, Optional.of(ok), Optional.empty());
            }
        } catch (SQLException ok) {
            return new Response<>(ResponseCode.ERROR, Optional.of(ok), Optional.empty());
        }
    }

    @Override
    public Response<Void> set(Transaction transaction) {
        try (DbStatement statement = db.createStatement()) {
            try {
                statement.startTransaction();
                if (!statement.inTransaction())
                    return new Response<>(ResponseCode.ERROR, Optional.of(new IllegalStateException("Currency set operation requiere transaction.")), Optional.empty());

                statement.executeUpdateQuery("INSERT INTO transaction(subjectId, currencyName, amount) VALUES(?, ?, ?) ON CONFLICT(subjectId, currencyName) DO UPDATE SET amount=?;", transaction.subjectIdentifier().toString(), transaction.currencyName(), transaction.amount(), transaction.amount());
                statement.commit();

                String key = transaction.subjectIdentifier().toString().concat(":").concat(transaction.currencyName());
                if(transactionCache.getIfPresent(key) != null) transactionCache.put(key, transaction.amount());
                return new Response<>(ResponseCode.OK, Optional.empty(), Optional.empty());
            } catch (SQLException ok) {
                statement.rollback();
                return new Response<>(ResponseCode.ERROR, Optional.of(ok), Optional.empty());
            }
        } catch (SQLException ok) {
            return new Response<>(ResponseCode.ERROR, Optional.of(ok), Optional.empty());
        }
    }

    /**
     * @param set
     * @return
     */
    @Override
    public @NotNull Response<Void> setAll(@NotNull Set<Transaction> set) {
        return null;
    }

    @Override
    public Response<Boolean> setThey(Set<Transaction> transactions) {
        if (transactions.isEmpty()) return new Response<>(ResponseCode.OK, Optional.empty(), Optional.of(true));
        try (DbStatement statement = db.createStatement()) {
            try {
                statement.startTransaction();
                if (!statement.inTransaction())
                    return new Response<>(ResponseCode.ERROR, Optional.of(new IllegalStateException("Currency set operation require transaction.")), Optional.of(false));

                Map<String, Double> keys = new HashMap<>((int) ((transactionCache.size() + transactions.size()) / 2));
                String key;
                for (Transaction transaction : transactions) {
                    statement.executeUpdateQuery("INSERT INTO transaction(subjectId, currencyName, amount) VALUE(?, ?, ?) ON CONFLICT(subjectId, currencyName) DO UPDATE SET amount=?;", transaction.subjectIdentifier().toString(), transaction.currencyName(), transaction.amount(), transaction.amount());
                    key = transaction.subjectIdentifier().toString().concat(":").concat(transaction.currencyName());
                    if(transactionCache.getIfPresent(key) != null) keys.put(key, transaction.amount());
                }
                statement.commit();

                transactionCache.putAll(keys);
                return new Response<>(ResponseCode.OK, Optional.empty(), Optional.of(true));
            } catch (SQLException ok) {
                statement.rollback();
                return new Response<>(ResponseCode.ERROR, Optional.of(ok), Optional.of(false));
            }
        } catch (SQLException ok) {
            return new Response<>(ResponseCode.ERROR, Optional.of(ok), Optional.of(false));
        }
    }
}
