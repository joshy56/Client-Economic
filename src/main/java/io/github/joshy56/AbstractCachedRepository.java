package io.github.joshy56;

import co.aikar.idb.Database;
import co.aikar.idb.DbStatement;
import com.google.common.cache.LoadingCache;
import io.github.joshy56.response.Response;
import io.github.joshy56.response.ResponseCode;
import org.jetbrains.annotations.NotNull;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Function;

/**
 * @author joshy56
 * @since 6/3/2024
 */
public abstract class AbstractCachedRepository<K, V> implements Repository<K, V> {
    private final UUID uniqueIdentifier;
    private final LoadingCache<K, V> cache;
    private final Database database;

    public AbstractCachedRepository(@NotNull Database database, @NotNull LoadingCache<K, V> cache) {
        this.uniqueIdentifier = UUID.randomUUID();
        this.database = database;
        this.cache = cache;
    }

    /**
     * Returns a hash code value for the object. This method is
     * supported for the benefit of hash tables such as those provided by
     * {@link HashMap}.
     * @return a hash code value for this object.
     * @implSpec As far as is reasonably practical, the {@code hashCode} method defined
     * by class {@code Object} returns distinct integers for distinct objects.
     * @see Object#equals(Object)
     * @see System#identityHashCode
     */
    @Override
    public int hashCode() {
        return uniqueIdentifier.hashCode();
    }

    /**
     * Indicates whether some other object is "equal to" this one.
     * @param obj the reference object with which to compare.
     * @return {@code true} if this object is the same as the obj
     * argument and not null; {@code false} otherwise.
     * @see #hashCode()
     * @see HashMap
     */
    @Override
    public boolean equals(Object obj) {
        if ((obj == null) || (obj.getClass() != AbstractCachedRepository.class)) return false;
        AbstractCachedRepository<?, ?> repository = (AbstractCachedRepository<?, ?>) obj;
        return repository.uniqueIdentifier.equals(this.uniqueIdentifier);
    }

    /**
     * Returns a string representation of the object.
     * @return a string representation of the object.
     */
    @Override
    public String toString() {
        StringBuilder string = new StringBuilder();
        string.append(getClass().getName())
                .append("@")
                .append(hashCode())
                .append(" {\n")
                .append("   id: '")
                .append(uniqueIdentifier)
                .append("'\n")
                .append("   cache: [");
        cache.asMap().entrySet().parallelStream().map(entry -> "{" + entry.getKey() + ", " + entry.getValue() + "}").forEach(string::append);
        string.append("]\n}");
        return string.toString();
    }

    /**
     * @param map
     * @return
     */
    protected <T> @NotNull Response<T> query(Function<DbStatement, Response<T>> map) {
        try (DbStatement statement = database.createStatement()) {
            try {
                statement.startTransaction();
                if (!statement.inTransaction())
                    return new Response<>(ResponseCode.ERROR, Optional.of(new RuntimeException("Something got wrong, need a open transaction.")), Optional.empty());

                Response<T> response = map.apply(statement);
                if (statement.inTransaction()) statement.commit();
                return response;
            } catch (SQLException ok) {
                statement.rollback();
                throw ok;
            }
        } catch (SQLException ok) {
            return new Response<>(ResponseCode.ERROR, Optional.of(new RuntimeException("Something got wrong, check it you SQL query.", ok)), Optional.empty());
        }
    }

    @NotNull
    protected LoadingCache<K, V> cache() {
        return cache;
    }

    @NotNull
    protected Database database() {
        return database;
    }
}
