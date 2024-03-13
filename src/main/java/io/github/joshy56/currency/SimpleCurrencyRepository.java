package io.github.joshy56.currency;

import co.aikar.idb.Database;
import co.aikar.idb.DbRow;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import io.github.joshy56.AbstractCachedRepository;
import io.github.joshy56.response.Response;
import io.github.joshy56.response.ResponseCode;
import org.intellij.lang.annotations.Language;
import org.jetbrains.annotations.NotNull;

import java.sql.SQLException;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * @author joshy56
 * @since 10/3/2024
 */
public class SimpleCurrencyRepository extends AbstractCachedRepository<String, Currency> implements CurrencyRepository {
    @Language("SQL")
    private final String sqlQueryGet, sqlQuerySet, sqlQueryDelete;
    public SimpleCurrencyRepository(@NotNull Database database) {
        super(database, CacheBuilder.newBuilder().expireAfterAccess(3, TimeUnit.MINUTES).expireAfterWrite(1, TimeUnit.MINUTES).build(
                new CacheLoader<>() {
                    @Override
                    public @NotNull Currency load(@NotNull String currencyName) throws Exception {
                        DbRow dbRow = database.getFirstRow("SELECT name, displayName, pluralName, abbreviation, symbol FROM currencies WHERE name=?;", currencyName);
                        return Optional.ofNullable(dbRow)
                                .map(row -> {
                                    Currency currency = new SimpleCurrency(row.getString("name"));
                                    currency.displayName(row.getString("displayName"));
                                    currency.displayNamePlural(row.getString("pluralName"));
                                    currency.abbreviation(row.getString("abbreviation"));
                                    currency.symbol(row.get("symbol"));
                                    return currency;
                                }).orElseThrow(() -> new NullPointerException("Subject don't exists."));
                    }
                }
        ));

        query(statement -> {
           try {
               statement.query("CREATE TABLE IF NOT EXISTS currencies(name VARCHAR(64) PRIMARY KEY NOT NULL, displayName VARCHAR(64), pluralName VARCHAR(64), abbreviation VARCHAR(3), symbol CHARACTER(1));");
               statement.executeUpdate();

               statement.commit();

               return Response.EMPTY();
           } catch (SQLException ok) {
               return Response.ERROR(ok);
           }
        });

        this.sqlQueryGet = "SELECT name, displayName, pluralName, abbreviation, symbol FROM currencies WHERE name=?;";
        this.sqlQuerySet = "INSERT INTO currencies(name, displayName, pluralName, abbreviation, symbol) VALUES(?, ?, ?, ?, ?) ON CONFLICT(name) DO UPDATE SET displayName=?, pluralName=?, abbreviation=?, symbol=?;";
        this.sqlQueryDelete = "DELETE FROM currencies WHERE name=?;";
    }

    /**
     * @param currencyName
     * @return
     */
    @Override
    public @NotNull Response<Currency> get(@NotNull String currencyName) {
        try {
            Currency subject = cache().get(currencyName);
            return new Response<>(ResponseCode.OK, Optional.empty(), Optional.of(subject));
        } catch (ExecutionException ok) {
            return new Response<>(ResponseCode.ERROR, Optional.of(new RuntimeException("Something got wrong, ups.", ok)), Optional.empty());
        }
    }

    /**
     * @param currenciesNames
     * @return
     */
    @Override
    public @NotNull Response<Set<Currency>> getAllOfThem(@NotNull Set<String> currenciesNames) {
        if (currenciesNames.isEmpty()) return new Response<>(ResponseCode.OK, Optional.empty(), Optional.empty());
        return query(statement -> {
            try {
                statement.query("BEGIN TRANSACTION;");
                statement.executeUpdate();
                for (String currencyName : currenciesNames) {
                    statement.query(sqlQueryGet);
                    statement.execute(currencyName);
                }
                statement.query("COMMIT;");
                statement.executeUpdate();

                statement.commit();

                Set<Currency> subjects = statement.getResults().parallelStream().map(row -> {
                    Currency currency = new SimpleCurrency(row.getString("name"));
                    currency.displayName(row.getString("displayName"));
                    currency.displayNamePlural(row.getString("pluralName"));
                    currency.abbreviation(row.getString("abbreviation"));
                    currency.symbol(row.get("symbol"));
                    return currency;
                }).collect(Collectors.toSet());
                if (subjects.isEmpty()) return new Response<>(ResponseCode.OK, Optional.empty(), Optional.empty());
                return new Response<>(ResponseCode.OK, Optional.empty(), Optional.of(subjects));
            } catch (SQLException ok) {
                return new Response<>(ResponseCode.ERROR, Optional.of(new RuntimeException("Uh oh, something got wrong.", ok)), Optional.empty());
            }
        });
    }

    /**
     * @return
     */
    @Override
    public @NotNull Response<Set<Currency>> getAll() {
        return query(statement -> {
            try {
                statement.query("SELECT * FROM currencies;");
                statement.executeUpdate();

                statement.commit();

                Set<Currency> currencies = statement.getResults().parallelStream().map(row -> {
                    Currency currency = new SimpleCurrency(row.getString("name"));
                    currency.displayName(row.getString("displayName"));
                    currency.displayNamePlural(row.getString("pluralName"));
                    currency.abbreviation(row.getString("abbreviation"));
                    currency.symbol(row.get("symbol"));
                    return currency;
                }).collect(Collectors.toSet());
                if (currencies.isEmpty()) return new Response<>(ResponseCode.OK, Optional.empty(), Optional.empty());
                return new Response<>(ResponseCode.OK, Optional.empty(), Optional.of(currencies));
            } catch (SQLException ok) {
                return new Response<>(ResponseCode.ERROR, Optional.of(new RuntimeException("Something got wrong, ups.", ok)), Optional.empty());
            }
        });
    }

    /**
     * @param currency
     * @return
     */
    @Override
    public @NotNull Response<Void> set(@NotNull Currency currency) {
        return query(statement -> {
            try {
                statement.query(sqlQuerySet);
                statement.executeUpdate(currency.name(), currency.displayName(), currency.displayNamePlural(), currency.abbreviation(), currency.symbol(), currency.displayName(), currency.displayNamePlural(), currency.abbreviation(), currency.symbol());

                statement.commit();

                return new Response<>(ResponseCode.OK, Optional.empty(), Optional.empty());
            } catch (SQLException ok) {
                return new Response<>(ResponseCode.ERROR, Optional.of(new RuntimeException("Oh...", ok)), Optional.empty());
            }
        });
    }

    /**
     * @param currencies
     * @return
     */
    @Override
    public @NotNull Response<Void> setAll(@NotNull Set<Currency> currencies) {
        if (currencies.isEmpty()) return new Response<>(ResponseCode.OK, Optional.empty(), Optional.empty());
        return query(statement -> {
            try {
                statement.query("BEGIN TRANSACTION;");
                statement.executeUpdate();
                for (Currency currency : currencies) {
                    statement.query(sqlQuerySet);
                    statement.executeUpdate(currency.name(), currency.displayName(), currency.displayNamePlural(), currency.abbreviation(), currency.symbol(), currency.displayName(), currency.displayNamePlural(), currency.abbreviation(), currency.symbol());
                }
                statement.query("COMMIT;");
                statement.executeUpdate();

                statement.commit();

                return new Response<>(ResponseCode.OK, Optional.empty(), Optional.empty());
            } catch (SQLException ok) {
                return new Response<>(ResponseCode.ERROR, Optional.of(new RuntimeException("That it's", ok)), Optional.empty());
            }
        });
    }

    /**
     * @param currencyName
     * @return
     */
    @Override
    public @NotNull Response<Void> delete(@NotNull String currencyName) {
        return query(statement -> {
            try {
                statement.query(sqlQueryDelete);
                statement.executeUpdate(currencyName);

                statement.commit();

                return new Response<>(ResponseCode.OK, Optional.empty(), Optional.empty());
            } catch (SQLException ok) {
                return new Response<>(ResponseCode.ERROR, Optional.of(new RuntimeException("I can't delete it...", ok)), Optional.empty());
            }
        });
    }

    /**
     * @param currenciesNames
     * @return
     */
    @Override
    public @NotNull Response<Void> deleteAllOfThem(@NotNull Set<String> currenciesNames) {
        return query(statement -> {
            try {
                statement.query("BEGIN TRANSACTION;");
                statement.executeUpdate();
                for (String currencyName : currenciesNames) {
                    statement.query(sqlQueryDelete);
                    statement.executeUpdate(currencyName);
                }
                statement.query("COMMIT;");
                statement.executeUpdate();

                statement.commit();

                return new Response<>(ResponseCode.OK, Optional.empty(), Optional.empty());
            } catch (SQLException ok) {
                return new Response<>(ResponseCode.ERROR, Optional.of(new RuntimeException("They're so power, can't delete then :p", ok)), Optional.empty());
            }
        });
    }

    /**
     * @return
     */
    @Override
    public @NotNull Response<Void> deleteAll() {
        return query(statement -> {
            try {
                statement.query("DELETE FROM currencies;");
                statement.executeUpdate();

                statement.commit();

                return new Response<>(ResponseCode.OK, Optional.empty(), Optional.empty());
            } catch (SQLException ok) {
                return new Response<>(ResponseCode.ERROR, Optional.of(new RuntimeException("They're so much, can't delete.", ok)), Optional.empty());
            }
        });
    }
}
